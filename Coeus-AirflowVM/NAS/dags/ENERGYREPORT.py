from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.daily_report_from_grafana import main
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


# Define Paths
OUTPUT_PATH = "/mnt/NAS-THELOCKER/daily_reports/energy/"

# Task 1 create the daily report
def generate_daily_report(**context):
    execution_date = context["data_interval_start"]
    data_date = execution_date.strftime("%Y-%m-%d")
    # Construct the input folder path with yesterday's date
    pdf_name = os.path.join(OUTPUT_PATH, f"Daily_OCG-DATA_Report-{data_date}.pdf")

    # Push the file path to XCom
    context["ti"].xcom_push(key="file_path", value=pdf_name)
    main(pdf_name)
    return


# Task 2 send email or push to share point
def email_report(**context):
    sender_email = "airflow@ocergy.com"
    receiver_emails = "flebrun@ocergy.com, n.roddier@tachyssema.fr, ntom@ocergy.com, ccermelli@ocergy.com, droddier@ocergy.com, mkumar@ocergy.com, vmulet@ocergy.com"
    cc_emails = ""
    smtp_server = "192.168.1.122"
    port = 587

    # Create the email
    message = MIMEMultipart("alternative")
    message["Subject"] = "OCG-DATA Daily Report"
    message["From"] = sender_email
    message["To"] = receiver_emails
    message["Cc"] = cc_emails

    # Add body to email
    report_date = context["ds"]
    text = f"Hello,\n\nDaily Report for {report_date} attached."
    part = MIMEText(text, "plain")
    message.attach(part)

    pdf_file_path = context["ti"].xcom_pull(
        task_ids="generate_daily_report", key="file_path"
    )

    # Open PDF file in binary mode and attach it to the email
    with open(pdf_file_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(pdf_file_path)}",
        )
        message.attach(part)

    # Connect to the server and send the email
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()
        rcpt = receiver_emails.split(", ") + [cc_emails]
        server.sendmail(sender_email, rcpt, message.as_string())
        # server.sendmail(sender_email, cc_emails, message.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server.quit()
    return


# Default arguments for the DAG
default_args = {
    "owner": "Valentin Mulet",
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# Define the DAG
with DAG(
    dag_id="Generate_Daily_Report",
    default_args=default_args,
    schedule_interval="55 6 * * *",
    tags=["monitoring"],
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task to list new files
    generate_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
        provide_context=True,
    )

    send_report = PythonOperator(
        task_id="email_report",
        python_callable=email_report,
        provide_context=True,
    )

    generate_report >> send_report
