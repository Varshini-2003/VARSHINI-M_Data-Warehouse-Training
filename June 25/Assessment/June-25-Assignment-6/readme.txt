replace 

AIRFLOW__SMTP__SMTP_USER: your_email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD: your_app_password

in the yaml file with your email and the password generated 

in security->2-step-verification-apppasswords

or visit https://myaccount.google.com/security

Select:

App: Mail

Device: Other → enter something like Airflow

Google will give you a 16-character password, e.g. abcd efgh ijkl mnop

Save this — it's your SMTP_PASSWORD.

environment:
  AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_STARTTLS: True
  AIRFLOW__SMTP__SMTP_SSL: False
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_MAIL_FROM: your_email@gmail.com
  AIRFLOW__SMTP__SMTP_USER: your_email@gmail.com
  AIRFLOW__SMTP__SMTP_PASSWORD: abcd efgh ijkl mnop  # your Gmail app password


Set Variable in Airflow UI
Open the Airflow UI → go to Admin → Variables

Click "+" to add a new variable

Set:
| Key                  | Value                                                |
| -------------------- | ---------------------------------------------------- |
| `notification_email` | [your\_email@gmail.com](mailto:your_email@gmail.com) |
