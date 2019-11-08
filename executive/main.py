# -*- coding: utf-8 -*-
import io
import datetime
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
    title = "%s Y%i M%i" % (title, max(df_plt['YEAR']), max(df_plt['MONTH']))
    ax.set_title(title, fontsize=16, fontweight="bold")


def genfig(query, gcspath, fontpath, service_json, run):
    '''load data'''
    start = datetime.datetime.now()
    df = gbq_load(query, service_json)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Load data from GBQ (%.1f mins)' % total_mins, run)
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
    logtxt('Generate figure (%.1f mins)' % total_mins, run)
    '''upload to gcs'''
    start = datetime.datetime.now()
    figfile = io.BytesIO()
    fig.savefig(figfile, bbox_inches='tight', format='png', dpi=100)
    figfile.seek(0)
    gcs_upload(figfile, gcspath, service_json, "image/png")
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
    imgurl1 = '/'.join(config['reportpath1'].split("/")[1:])
    # prepare text
    body_header = '''Report on %s

    ''' % td

    body_footer = '''Dashboard please follow <a href="%s" target="_blank">link</a>

Best Regards,
Digital Intelligence, Digital Office
SCG Cement-Building Materials Co., Ltd.

    ''' % dashboard_url

    input_json = {
        "from": mail_from,
        "to": mail_to,
        "subject": "[DURA Executive Summary] Report on %s" % td,
        "body_header": body_header,
        "body_footer": body_footer,
        "img_list": [imgurl1],
        "attach_list": []
    }

    requests.post(url_request, json=input_json, headers=headers)
    total_mins = (datetime.datetime.now() - start).total_seconds() / 60
    logtxt('Send mail (%.1f mins)' % total_mins, run)


def main(config, fontpath):
    # set variables
    run = config['run']
    service_json = config['gcpauth']
    with gcs_download(config['sqlpath1'], service_json) as sql_file:
        query1 = sql_file.read().decode('ascii')
    # run
    start_time = datetime.datetime.now()
    logtxt("Start process", run)
    try:
        genfig(query1, config['reportpath1'], fontpath, service_json, run)
        sendmail(config, run)
    except Exception as e:
        logtxt("ERROR (%s)" % str(e), run, True)
    end_time = datetime.datetime.now()
    total_mins = (end_time - start_time).total_seconds() / 60
    logtxt('Total time (%.1f mins)' % total_mins, run)


if __name__ == "__main__":
    with open("config.yaml") as f:
        config = yaml.load(f, Loader=yaml.Loader)
    main(config, "arial.ttf")
