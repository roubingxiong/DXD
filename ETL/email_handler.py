#!/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from email.header import Header
import email
import time
import ConfigParser
import os
import logging
logger = logging.getLogger('DXD_ETL')

global email_count
email_count = 2

def write_report_email(messager={}, attachment=[]):
    table = messager['table_name']
    action = messager['action']
    date = messager['from_date']
    status = messager['status']
    data_file = messager['data_file']
    ctrl_file =messager['ctrl_file']
    ctrl_count = messager['ctrl_count']

    subject = action + ' ' + table + ' ' + status

    table_subject = table + ' - ' + date

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
        <th class="tg-amwm" colspan="8">%s</th>
      </tr>
      <tr>
        <td class="tg-v4ss">No</td>
        <td class="tg-v4ss">Action</td>
        <td class="tg-v4ss">table</td>
        <td class="tg-v4ss">data_file</td>
        <td class="tg-v4ss">ctrl_file</td>
        <td class="tg-v4ss">row_count</td>
        <td class="tg-v4ss">status</td>
      </tr>""" % (table_subject)


    table_body = ''''''
    rowNum = 1

    html_table_row_body = '''
        <td class="tg-yw4l">%s</td>
        <td class="tg-yw4l">%s</td>
        <td class="tg-yw4l">%s</td>
        <td class="tg-lqy6">%s</td>
        <td class="tg-lqy6">%s</td>
        <td class="tg-lqy6">%s</td>
        ''' % (rowNum, action, table, data_file, ctrl_file, ctrl_count)

    if status == 'Fail':
        html_table_row_status = '''<td class="tg-0fim">%s</td>''' % (status)
    else:
        html_table_row_status = '''<td class="tg-ix49">%s</td>''' % (status)

    html_table_row_end = "</tr>"

    table_body = table_header + html_table_row_body + html_table_row_status + html_table_row_end

    table_comment = """
        <tr>
            <td class="tg-yw4l" colspan="8">Comments：测试中</td>
          </tr>
        """

    table_end = """</table>"""

    mail_msg = mail_style + mail_header + table_begin +table_body + table_comment + table_end + mail_footer

    if len(attachment) > 0:
        message = MIMEMultipart()

        message.attach( MIMEText(mail_msg, 'html', 'utf-8'))

        for attach in attachment:
            attach_basename = os.path.basename(attach)
            # construct attachments
            att = MIMEText(open(attach, 'rb').read(), 'base64', 'utf-8')
            att["Content-Type"] = 'application/octet-stream'
            # attachment display name
            att["Content-Disposition"] = 'attachment; filename=' + attach_basename
            message.attach(att)
            print 'attached ->' + attach
    else:
        message = MIMEText(mail_msg, 'html', 'utf-8')#MIMEText('Python 邮件发送测试...', 'plain', 'utf-8')

    message['Subject'] = subject

    return message


def sendJobStatusEmail(messager={}, attachment=[]):

    wait = 20
    try:
        sendEmail(messager, attachment)
    except Exception as e:
        logger.exception(e.message)
        logger.warn('exception occured while sending email, resend it %s seconds later', wait)
        time.sleep(wait)
        sendEmail(messager, attachment)

def sendEmail(messager={}, attachment=[]):
    # print messager
    config = messager['config']
    mail_host=config.get('email','host')  #设置服务器
    mail_user=config.get('email','user')    #用户名
    mail_pass=config.get('email','passwd')   #口令

    sender = mail_user
    receivers = config.get('email','to')#.split(';')#['roubingxiong@163.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    message = write_report_email(messager=messager, attachment=attachment)
    message['From'] = mail_user
    message['To'] = receivers

    try:
        logger.info('sending %s email notification', messager['status'])
        smtpObj = smtplib.SMTP()
        # smtpObj.connect(mail_host, 25)    # 端口号 163
        smtpObj.connect(mail_host, 587)    # hotmail
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.ehlo()
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())

    except smtplib.SMTPException as e:
        logger.exception(e.message)
        logger.info('failed to send %s email notification', messager['status'])
        raise
    else:
        logger.info('send email successfully')