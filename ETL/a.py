# -*- coding: UTF-8 -*-
#!/usr/bin/python
import email
import smtplib

msg = email.message_from_string('''552 Requested mail action aborted: exceeded mailsize limit 发送的信件大小超过了网易邮箱允许接收的最大限制；
　　•553 Requested action not taken: NULL sender is not allowed 不允许发件人为空，请使用真实发件人发送；
　　•553 Requested action not taken: Local user only  SMTP类型的机器只允许发信人是本站用户；
　　•553 Requested action not taken: no smtp MX only  MX类型的机器不允许发信人是本站用户；''')
msg["From"] = "roubingxiong@hotmail.com"
msg['To'] = "roubingxiong@163.com;xiongzhibin1987@126.com"
msg['Subject'] = "你好"

s = smtplib.SMTP("smtp.live.com",587)
s.ehlo()
s.starttls()
s.ehlo()
s.login('roubingxiong@hotmail.com', 'roubing2017')


s.sendmail("roubingxiong@hotmail.com", msg['To'], msg.as_string())

s.quit()

print msg.as_string()

