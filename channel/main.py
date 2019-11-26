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
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from google.cloud import storage
from google.cloud import bigquery


parser = argparse.ArgumentParser()
parser.add_argument("-l", "--local", action="store_true", help="run locally")
args = parser.parse_args()


def logtxt(log, runlocal=False, error=False):
    if runlocal:
        print("ERROR!! %s" % log) if error else print(log)
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


def gcs_upload(file, gcspath, service_json="None", contenttype=None, public=False):
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


def autolabel(ax):
    for rect in ax.patches:
        height = rect.get_height()
        ax.annotate('{:,.1f}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


def txt_arrow(txt, val):
    uparrow = '\u25B2'
    downarrow = '\u25BC'
    upcolor = "tab:green"
    downcolor = "tab:red"
    if val >= 0:
        fontcolor = upcolor
        arrow = uparrow
    else:
        fontcolor = downcolor
        arrow = downarrow
    maxlen = 14
    totallen = len(txt) + len('{:+.1%}'.format(val)) + len(arrow)
    space = " " * (maxlen-totallen)
    return '''%s %s%s %s''' % (txt, space, '{:+.1%}'.format(val), arrow), fontcolor

     
def txt_percent(font, ax, df, scope, x_pos, y_pos=0.9):
    font = fm.FontProperties(fname=font)
    # get percentage
    df = df[df['Region3_Division']==scope].copy()
    gr = df['GR'].values[0]
    var = df['VAR'].values[0]
    est = df['EST'].values[0]
    # growth
    txt, fontcolor = txt_arrow("GR", gr)
    ax.text(x_pos, ax.get_ylim()[1] * y_pos, txt, color=fontcolor, fontproperties=font, size=14)
    # var
    txt, fontcolor = txt_arrow("VAR", var)
    ax.text(x_pos, ax.get_ylim()[1] * (y_pos-0.06), txt, color=fontcolor, fontproperties=font, size=14)
    # est
    txt, fontcolor = txt_arrow("EST", est)
    ax.text(x_pos, ax.get_ylim()[1] * (y_pos-0.12), txt, color=fontcolor, fontproperties=font, size=14)


def plt_bargroup(ax, df, title, font):
    # set variables
    barWidth = 0.25
    gr_col = "Region3_Division"
    gr_list = ["Total", "Moderntrade", "Dealer"]
    gr_order = {x: y for x, y in zip(gr_list, range(len(gr_list)))}
    plt_list = ["SALES_LY", "SALES_TG", "SALES_ACT"]
    plt_color = ["tab:grey", "tab:orange", "tab:blue"]
    # prepare data frame
    df_plt = df.copy()
    df_plt['order'] = df_plt[gr_col].map(gr_order)
    df_plt = df_plt.sort_values(by='order', ascending=True).reset_index(drop=True)
    # plot ax
    for i, x in enumerate(plt_list):
        r = [x + barWidth * i for x in range(len(plt_list))]
        ax.bar(r, df_plt[x], color=plt_color[i], width=barWidth, label=x)
    # show percent text
    ax.set_ylim(top = ax.get_ylim()[1] * 1.4)
    for i, x in enumerate(gr_list):
        txt_percent(font, ax, df_plt, x, i)
    autolabel(ax)
    ax.set_xticks([r + barWidth for r in range(len(df_plt))])
    ax.set_xticklabels(list(gr_order.keys()), fontsize=14)
    ax.set_ylabel("Net Amount (MB)", fontsize=14)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.07), ncol=len(plt_list))
    updatedate = "latest data: %s" % max(df_plt['UPDATE_DATE']).strftime("%d %b %Y")
    ax.text(ax.get_xlim()[1]-0.5, ax.get_ylim()[1], updatedate, size=12)
    title = "%s Y%i M%i" % (title, max(df_plt['Year']), max(df_plt['Month']))
    ax.set_title(title, fontsize=16, fontweight="bold")


def genfig(query, gcspath, fontpath, service_json, runlocal):
    '''load data'''
    start = datetime.datetime.now()
    df = gbq_load(query, service_json)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Load data from GBQ (%.1f mins)' % total_mins, runlocal)
    '''create figure'''
    # set styling
    start = datetime.datetime.now()
    sns.set_style('dark')
    sns.set(rc={'figure.figsize':(15, 15)})
    # create plot
    fig = plt.figure()
    # plt graph
    ax1 = fig.add_subplot(211)
    plt_bargroup(ax1, df, "Sales Summary", fontpath)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Generate figure (%.1f mins)' % total_mins, runlocal)
    '''upload to gcs'''
    start = datetime.datetime.now()
    figfile = io.BytesIO()
    fig.savefig(figfile, bbox_inches='tight', format='png', dpi=100)
    figfile.seek(0)
    gcs_upload(figfile, gcspath, service_json, "image/png")
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Upload to GCS (%.1f mins)' % total_mins, runlocal)


def loadtoexcel(query, excelpath, rawfilename, reportpath, service_json="None", runlocal=False):
    # load excel file from gcs
    start = datetime.datetime.now()
    file_outlet_name = excelpath.split("/")[-1]
    file_outlet = gcs_download(excelpath, service_json)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Load excel file from GCS (%.1f mins)' % total_mins, runlocal)
    # load data from Google BigQuery
    start = datetime.datetime.now()
    df = gbq_load(query, service_json)
    file_raw = io.StringIO()
    df.to_csv(file_raw, encoding='utf-8', index=False)
    file_raw.seek(0)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Load data from GBQ (%.1f mins)' % total_mins, runlocal)
    # zip file
    start = datetime.datetime.now()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr(file_outlet_name, file_outlet.getvalue())
        zip_file.writestr(rawfilename, file_raw.getvalue())
    zip_buffer.seek(0)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Zip file (%.1f mins)' % total_mins, runlocal)
    # upload to Google Cloud Storage
    start = datetime.datetime.now()
    gcs_upload(zip_buffer, reportpath, service_json, None, True)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('[Channel] Upload to GCS (%.1f mins)' % total_mins, runlocal)


def sendmail(webapi, apikey, mailpath, reportpath1, reportpath2, service_json="None", runlocal=False):
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
    imgurl1 = '/'.join(reportpath1.split("/")[1:])
    downloadurl = "https://storage.googleapis.com" + "/".join(reportpath2.split("/")[1:])
    # prepare text
    subject = "[MTB Channel] Report on %s" % td
    body_header = '''Report on %s

    ''' % (td)

    body_footer = '''Outlet report can be downloaded <a href="%s">here</a>

Best Regards,
Digital Intelligence, Digital Office
SCG Cement-Building Materials Co., Ltd.

    ''' % downloadurl

    input_json = {
        "from": mail_from,
        "to": mail_to,
        "subject": subject,
        "body_header": body_header,
        "body_footer": body_footer,
        "img_list": [imgurl1],
        "attach_list": []
    }

    r = requests.post(url_request, json=input_json, headers=headers)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    if r.status_code == 200:
        logtxt('[Channel] Send mail (%.1f mins)' % total_mins, runlocal)
    else:
        logtxt('[Channel] ERROR Send mail: %s (%.1f mins)' % (r.status_code, total_mins), runlocal, True)


def main(fontpath):
    # set variables
    runlocal = args.local
    service_json = os.environ['gcpauth']
    with gcs_download(os.environ['channel_sqlpath1'], service_json) as sql_file:
        query1 = sql_file.read().decode('ascii')
    reportpath1 = os.environ['channel_reportpath1']
    with gcs_download(os.environ['channel_sqlpath2'], service_json) as sql_file:
        query2 = sql_file.read().decode('ascii')
    excelpath2 = os.environ['channel_excelpath2']
    rawfilename2 = os.environ['channel_rawfilename2']
    reportpath2 = os.environ['channel_reportpath2']
    email_api = os.environ["email_api"]
    email_apikey = os.environ["email_apikey"]
    mailpath = os.environ['channel_mailpath']
    # run
    start_time = datetime.datetime.now()
    logtxt("[Channel] Start process", runlocal)
    try:
        genfig(query1, reportpath1, fontpath, service_json, runlocal)
        loadtoexcel(query2, excelpath2, rawfilename2, reportpath2, service_json, runlocal)
        sendmail(email_api, email_apikey, mailpath, reportpath1, reportpath2, service_json, runlocal)
    except Exception as e:
        logtxt("[Channel] ERROR (%s)" % str(e), runlocal, True)
    end_time = datetime.datetime.now()
    total_mins = (end_time - start_time).total_seconds() / 60
    logtxt('[Channel] Total time (%.1f mins)' % total_mins, runlocal)


if __name__ == "__main__":
    if args.local:
        with open("config.yaml") as f:
            config = yaml.load(f, Loader=yaml.Loader)
        for k, v in config.items():
            os.environ[k] = v
    main(fontpath="arial.ttf")
