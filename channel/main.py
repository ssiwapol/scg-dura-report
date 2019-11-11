# -*- coding: utf-8 -*-
import os
import io
import datetime
import zipfile
import argparse
import logging

import requests
import yaml
from pytz import timezone
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery


parser = argparse.ArgumentParser()
parser.add_argument("-l", "--local", action="store_true", help="run locally")
args = parser.parse_args()


def logtxt(log, runlocal=False, error=False):
    if runlocal:
        print("ERROR: %s" % log) if error else print(log)
    else:
        logging.error(log) if error else logging.info(log)


def gcs_download(gcspath, service_json="None"):
    if service_json == "None":
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json(service_json)
    bucket = client.get_bucket(gcspath.split("/")[2])
    fullpath = '/'.join(gcspath.split("/")[3:])
    blob = storage.Blob(fullpath, bucket)
    blob = storage.Blob(fullpath, bucket)
    byte_stream = io.BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    return byte_stream


def gcs_upload(file, gcspath, service_json="None", public=False, contenttype=None):
    if service_json == "None":
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json(service_json)
    bucket = client.get_bucket(gcspath.split("/")[2])
    fullpath = '/'.join(gcspath.split("/")[3:])
    blob = storage.Blob(fullpath, bucket)
    if contenttype is None:
        blob.upload_from_file(file)
    else:
        blob.upload_from_file(file, content_type=contenttype)
    if public:
        blob.make_public()


def gbq_load(query, service_json="None"):
    if service_json == "None":
        client = bigquery.Client()
    else:
        client = bigquery.Client.from_service_account_json(service_json)
    df = client.query(query).to_dataframe()
    return df


def loadtoexcel(query, excelpath, rawfilename, reportpath, service_json="None", runlocal=False):
    # load excel file from gcs
    start = datetime.datetime.now()
    file_outlet_name = excelpath.split("/")[-1]
    file_outlet = gcs_download(excelpath, service_json)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Load excel file from GCS (%.1f mins)' % total_mins, runlocal)
    # load data from Google BigQuery
    start = datetime.datetime.now()
    df = gbq_load(query, service_json)
    file_raw = io.StringIO()
    df.to_csv(file_raw, encoding='utf-8', index=False)
    file_raw.seek(0)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Load data from GBQ (%.1f mins)' % total_mins, runlocal)
    # zip file
    start = datetime.datetime.now()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr(file_outlet_name, file_outlet.getvalue())
        zip_file.writestr(rawfilename, file_raw.getvalue())
    zip_buffer.seek(0)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Zip file (%.1f mins)' % total_mins, runlocal)
    # upload to Google Cloud Storage
    start = datetime.datetime.now()
    gcs_upload(zip_buffer, reportpath, service_json, True)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Upload to GCS (%.1f mins)' % total_mins, runlocal)


def sendmail(webapi, apikey, mailpath, reportpath1, service_json="None", runlocal=False):
    start = datetime.datetime.now()
    dtnow = datetime.datetime.now(timezone('Asia/Bangkok'))
    td = dtnow.strftime("%d %b %Y")
    # prepare data
    url_request = webapi
    headers ={"apikey": apikey}
    with gcs_download(mailpath, service_json) as f:
        mail_data = yaml.load(f, Loader=yaml.Loader)
        mail_from = mail_data['from']
        mail_to = mail_data['to']
        dashboard_url = mail_data['dashboard_url']
    downloadurl = "https://storage.googleapis.com" + "/".join(reportpath1.split("/")[1:])
    # prepare text
    body_header = '''Report on %s
Dashboard please follow <a href="%s" target="_blank">link</a>
    ''' % (td, dashboard_url)

    body_footer = '''Outlet report can download <a href="%s">here</a>

Best Regards,
Digital Intelligence, Digital Office
SCG Cement-Building Materials Co., Ltd.

    ''' % downloadurl

    input_json = {
        "from": mail_from,
        "to": mail_to,
        "subject": "[DURA Channel Summary] Report on %s" % td,
        "body_header": body_header,
        "body_footer": body_footer,
        "img_list": [],
        "attach_list": []
    }

    requests.post(url_request, json=input_json, headers=headers)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Send mail (%.1f mins)' % total_mins, runlocal)


def main():
    # set variables
    runlocal = args.local
    service_json = os.environ['gcpauth']
    with gcs_download(os.environ['channel_sqlpath1'], service_json) as sql_file:
        query1 = sql_file.read().decode('ascii')
    excelpath1 = os.environ['channel_excelpath1']
    rawfilename1 = os.environ['channel_rawfilename1']
    reportpath1 = os.environ['channel_reportpath1']
    email_api = os.environ["email_api"]
    email_apikey = os.environ["email_apikey"]
    mailpath = os.environ['channel_mailpath']
    # run
    start_time = datetime.datetime.now()
    logtxt("Start process", runlocal)
    try:
        loadtoexcel(query1, excelpath1, rawfilename1, reportpath1, service_json, runlocal)
        sendmail(email_api, email_apikey, mailpath, reportpath1, service_json, runlocal)
    except Exception as e:
        logtxt("ERROR (%s)" % str(e), runlocal, True)
    end_time = datetime.datetime.now()
    total_mins = (end_time - start_time).total_seconds() / 60
    logtxt('Total time (%.1f mins)' % total_mins, runlocal)


if __name__ == "__main__":
    if args.local:
        with open("config.yaml") as f:
            config = yaml.load(f, Loader=yaml.Loader)
        for k, v in config.items():
            os.environ[k] = v
    main()
