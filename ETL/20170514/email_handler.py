#!/usr/bin/env python2
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


def write_report_email(messager=[], attachment=[]):
    msg_list = []
    row_num = 1
    global from_date
    global to_date
    global action_type
    global run_mode

    for msg in messager:
        table = msg['table_name']
        action_type = msg['action_type']
        from_date = msg['from_date']
        to_date = msg['to_date']
        status = msg['status']
        data_file = msg['data_file']
        ctrl_file =msg['ctrl_file']
        ctrl_count = msg['ctrl_count']
        run_mode = msg['run_mode']
        msg_list.append((row_num, action_type, table, data_file, ctrl_file, ctrl_count, status))

        row_num = row_num + 1

    subject = "ETL[%s] Status Report - %s[%s]"%(action_type, to_date, run_mode)

    table_subject = "ETL Status Report [%s - %s]"%(from_date, to_date)

    mail_style = """
    <style type="text/css">
        .tg  {border-collapse:collapse;border-spacing:0;border-color:#aabcfe;}
        .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#aabcfe;color:#669;background-color:#e8edff;}
        .tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#aabcfe;color:#039;background-color:#b9c9fe;}
        .tg .tg-h5s3{background-color:#f8ff00;vertical-align:top}
        .tg .tg-mb3i{background-color:#D2E4FC;text-align:right;vertical-align:top}
        .tg .tg-v4ss{background-color:#D2E4FC;font-weight:bold;vertical-align:top}
        .tg .tg-lqy6{text-align:left;vertical-align:top}
        .tg .tg-count{text-align:right;vertical-align:top}
        .tg .tg-amwm{font-weight:bold;text-align:center;vertical-align:top}
        .tg .tg-yw4l{vertical-align:top}
        .tg .tg-6k2t{background-color:#D2E4FC;vertical-align:top}
        .tg .tg-fail{background-color:#fd6864;text-align:center;vertical-align:top}
        .tg .tg-success{background-color:#32cb00;text-align:center;vertical-align:top}
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
        <td class="tg-v4ss">action_type</td>
        <td class="tg-v4ss">table</td>
        <td class="tg-v4ss">data_file</td>
        <td class="tg-v4ss">ctrl_file</td>
        <td class="tg-v4ss">row_count</td>
        <td class="tg-v4ss">status</td>
      </tr>""" % table_subject

    table_rows = ''''''

    for data in msg_list:   #draw each row
        status = data[len(data)-1]

        if status == 'Fail':
            table_row = '''
            <tr>
            <td class="tg-yw4l">%s</td>
            <td class="tg-yw4l">%s</td>
            <td class="tg-yw4l">%s</td>
            <td class="tg-lqy6">%s</td>
            <td class="tg-lqy6">%s</td>
            <td class="tg-count">%s</td>
            <td class="tg-fail">%s</td>
            </tr>
            ''' % data
        elif status == 'Success':
            table_row = '''
            <tr>
            <td class="tg-yw4l">%s</td>
            <td class="tg-yw4l">%s</td>
            <td class="tg-yw4l">%s</td>
            <td class="tg-lqy6">%s</td>
            <td class="tg-lqy6">%s</td>
            <td class="tg-count">%s</td>
            <td class="tg-success">%s</td>
            </tr>
            ''' % data
        else:
            pass

        table_rows = table_rows + table_row

    table_comment = """
        <tr>
            <td class="tg-yw4l" colspan="8">Comments：the extract/load do not include the data updated on %s</td>
          </tr>
        """ % to_date

    table_end = """</table>"""

    mail_msg = mail_style + mail_header + table_begin + table_header + table_rows + table_comment + table_end + mail_footer

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
            logger.info('attaching file ->%s',attach)
    else:
        message = MIMEText(mail_msg, 'html', 'utf-8')

    message['Subject'] = subject

    return message


def sendJobStatusEmail(config, messager=[], attachment=[]):

    wait = 20
    try:
        sendEmail(config, messager, attachment)
    except Exception as e:
        logger.exception(e.message)
        logger.warn('exception occured while sending email, resend it %s seconds later', wait)
        time.sleep(wait)
        sendEmail(config, messager, attachment)

def sendEmail(config, messager=[], attachment=[]):
    # print messager

    mail_host=config.get('email','host')  
    mail_user=config.get('email','user')    
    mail_pass=config.get('email','passwd')   

    sender = mail_user
    receivers = config.get('email','to')#.split(';')#['roubingxiong@163.com']  

    message = write_report_email(messager=messager, attachment=attachment)
    message['From'] = mail_user
    message['To'] = receivers

    try:
        logger.info('sending status email notification')
        smtpObj = smtplib.SMTP()
        # smtpObj.connect(mail_host, 25)    
        smtpObj.connect(mail_host, 587)    # hotmail
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.ehlo()
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        smtplib.SMTPDataError
    except smtplib.SMTPException as e:
        logger.exception(e.message)
        logger.info('failed to send status email notification')
        raise
    else:
        logger.info('send email successfully')


