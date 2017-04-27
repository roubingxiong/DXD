#!/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
from email.mime.text import MIMEText
from email.header import Header
import email
import time
import ConfigParser
import os
import logging
logger = logging.getLogger('DXD_ETL')

# 第三方 SMTP 服务
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.getcwd(),'init.cfg'))

mail_host=config.get('email','host')  #设置服务器
mail_user=config.get('email','user')    #用户名
mail_pass=config.get('email','passwd')   #口令

sender = mail_user
receivers = config.get('email','to').split(';')#['roubingxiong@163.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱


def write_report_email(subject, runDate, statusList):
    global message

    mail_style = """
    <style type="text/css">
        .tg  {border-collapse:collapse;border-spacing:0;border-color:#aabcfe;}
        .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#aabcfe;color:#669;background-color:#e8edff;}
        .tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#aabcfe;color:#039;background-color:#b9c9fe;}
        .tg .tg-h5s3{background-color:#f8ff00;vertical-align:top}
        .tg .tg-mb3i{background-color:#D2E4FC;text-align:right;vertical-align:top}
        .tg .tg-v4ss{background-color:#D2E4FC;font-weight:bold;vertical-align:top}
        .tg .tg-lqy6{text-align:right;vertical-align:top}
        .tg .tg-amwm{font-weight:bold;text-align:center;vertical-align:top}
        .tg .tg-yw4l{vertical-align:top}
        .tg .tg-6k2t{background-color:#D2E4FC;vertical-align:top}
        .tg .tg-0fim{background-color:#fd6864;text-align:center;vertical-align:top}
        .tg .tg-ix49{background-color:#32cb00;text-align:center;vertical-align:top}
    </style>
    """
    mail_header = """

    """
    mail_footer = """

    """
    table_begin = """<table class="tg">"""

    table_header = """
      <tr>
        <th class="tg-amwm" colspan="8">%s - %s</th>
      </tr>
      <tr>
        <td class="tg-v4ss">No</td>
        <td class="tg-v4ss">prod_table</td>
        <td class="tg-v4ss">data_file</td>
        <td class="tg-v4ss">ctrl_file</td>
        <td class="tg-v4ss">extract_row_count</td>
        <td class="tg-v4ss">bi_table</td>
        <td class="tg-v4ss">load_row_count</td>
        <td class="tg-v4ss">status</td>
      </tr>""" % (subject, runDate)


    table_body = ''''''
    rowNum = 0
    for row in statusList:
        rowNum = rowNum + 1

        html_table_row_begin = "<tr>"

        html_table_row_body = '''
        <td class="tg-yw4l">%s</td>
        <td class="tg-yw4l">%s</td>
        <td class="tg-lqy6">%s</td>
        <td class="tg-lqy6">%s</td>
        <td class="tg-lqy6">%s</td>
        <td class="tg-lqy6">%s</td>
        <td class="tg-yw4l">%s</td>
        ''' % (rowNum, row['prod_table'], row['data_file_name'], row['cntrl_file_name'], row['extract_row_count'], row['bi_table'], row['load_row_count'])

        if row['extract_row_count'] == row['load_row_count']:
            html_table_row_status = '''<td class="tg-ix49">%s</td>''' % ('成功')
        else:
            html_table_row_status = '''<td class="tg-0fim">%s</td>''' % ('出错')

        html_table_row_end = "</tr>"

        table_body = table_body + html_table_row_begin + html_table_row_body + html_table_row_status + html_table_row_end

    table_comment = """
        <tr>
            <td class="tg-yw4l" colspan="8">Comments：测试中</td>
          </tr>
        """

    table_end = """</table>"""

    mail_msg = mail_style + mail_header + table_begin + table_header + table_body + table_comment +table_end + mail_footer

    # message = email.message_from_string(mail_msg)
    message = MIMEText(mail_msg, 'html', 'utf-8')#MIMEText('Python 邮件发送测试...', 'plain', 'utf-8')

    return message

statusList = [
    {'prod_table': 'prod_table',
     'data_file_name': 'data_file_name',
     'cntrl_file_name': 'cntrl_file_name',
     'extract_row_count': 1000,
     'bi_table': 'bi_table',
     'load_row_count': 1000,
     'status': 'status'},
    {'prod_table': 'prod_table',
     'data_file_name': 'data_file_name',
     'cntrl_file_name': 'cntrl_file_name',
     'extract_row_count': 1000,
     'bi_table': 'bi_table',
     'load_row_count': 1001,
     'status': 'status'}
]
messag = write_report_email(subject='BI ETL 状态报告', runDate='2017-04-23', statusList=statusList)
message['From'] = "roubingxiong@hotmail.com"
message['To'] = "roubingxiong@163.com"
message['Subject'] = 'BI ETL 状态报告'

def sendEmail():
    logger.info('sending email to: %s', message['To'])
    try:
        smtpObj = smtplib.SMTP()
        # smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.connect(mail_host, 587)    # 25 为 SMTP 端口号
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.ehlo()
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        logger.info('send email successfully')
    except smtplib.SMTPException as e:
        logger.exception(e.message)
        logger.error('Bad luck, failed to send email')
        raise