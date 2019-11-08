# -*- coding: utf-8 -*-
import io
import datetime
import zipfile
import logging

import requests
import yaml
from pytz import timezone
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery


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


def logtxt(log, run="local", error=False):
    if run == "local":
        print("ERROR: %s" % log) if error else print(log)
    else:
        logging.info(log) if error is True else logging.error(log)


def loadtoexcel(query, excelpath, rawfilename, reportpath, service_json, run):
    # load excel file from gcs
    start = datetime.datetime.now()
    file_outlet_name = excelpath.split("/")[-1]
    file_outlet = gcs_download(excelpath, service_json)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Load excel file from GCS (%.1f mins)' % total_mins, run)
    # load data from Google BigQuery
    start = datetime.datetime.now()
    df = gbq_load(query, service_json)
    file_raw = io.StringIO()
    df.to_csv(file_raw, encoding='utf-8', index=False)
    file_raw.seek(0)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Load data from GBQ (%.1f mins)' % total_mins, run)
    # zip file
    start = datetime.datetime.now()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr(file_outlet_name, file_outlet.getvalue())
        zip_file.writestr(rawfilename, file_raw.getvalue())
    zip_buffer.seek(0)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Zip file (%.1f mins)' % total_mins, run)
    # upload to Google Cloud Storage
    start = datetime.datetime.now()
    gcs_upload(zip_buffer, reportpath, service_json, True)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Upload to GCS (%.1f mins)' % total_mins, run)


def sendmail(config, run):
    start = datetime.datetime.now()
    dtnow = datetime.datetime.now(timezone('Asia/Bangkok'))
    td = dtnow.strftime("%d %b %Y")
    # prepare data
    url_request = config["email_api"]
    headers ={"apikey": config["email_apikey"]}
    with gcs_download(config['mailpath'], config["gcpauth"]) as f:
        mail_data = yaml.load(f, Loader=yaml.Loader)
        mail_from = mail_data['from']
        mail_to = mail_data['to']
        dashboard_url = mail_data['dashboard_url']
    downloadurl = "https://storage.googleapis.com" + "/".join(config['reportpath1'].split("/")[1:])
    # prepare text
    body_header = '''Report on %s
For dashboard please follow <a href="%s" target="_blank">link</a>
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
    logtxt('Send mail (%.1f mins)' % total_mins, run)
    
def main():
    # set variables
    with open("config.yaml") as f:
        config = yaml.load(f, Loader=yaml.Loader)
    run = config['run']
    service_json = config['gcpauth']
    with gcs_download(config['sqlpath1'], service_json) as sql_file:
        query1 = sql_file.read().decode('ascii')
    # run
    start_time = datetime.datetime.now()
    logtxt("Start process", run)
    try:
        loadtoexcel(query1, config['excelpath1'], config['rawfilename1'], config['reportpath1'], service_json, run)
        sendmail(config, run)
    except Exception as e:
        logtxt("ERROR (%s)" % str(e), run, True)
    end_time = datetime.datetime.now()
    total_mins = (end_time - start_time).total_seconds() / 60
    logtxt('Total time (%.1f mins)' % total_mins, run)


if __name__ == "__main__":
    main()
