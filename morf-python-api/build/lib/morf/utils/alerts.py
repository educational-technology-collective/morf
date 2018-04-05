# Copyright (c) 2018 The Regents of the University of Michigan
# and the University of Pennsylvania
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Functions for sending alerts to users/owners/developers based on MORF jobs and usage.
"""
from morf.utils import download_from_s3, generate_archive_filename, make_s3_key_path
import boto3
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import io


def construct_message_body(job_config, docs_url = "https://jpgard.github.io/morf/",
                           emailaddr_info="morf-info@umich.edu"):
    job_id = job_config.job_id
    status = job_config.status
    message_body = """

        This is an automated notification that execution for your job on MORF for job_id {} has successfully completed status {}.
        
        Additional notifications will be provided as your job completes each step of the workflow.

        If you need support with MORF, check the MORF documentation at {} or contact the maintainers at {}.

        - The MORF team
        """.format(job_id, status, docs_url, emailaddr_info)
    return message_body


def construct_message_subject(job_config):
    subject = "MORF notification: job {}".format(job_config.job_id)
    return subject


def ses_send_email(job_config, emailaddr_from, subject, body):
    """
    Send email via ses.
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param emailaddr_to:
    :param emailaddr_from:
    :param subject:
    :param body:
    :param status:
    :return:
    """
    aws_access_key_id = job_config.aws_access_key_id
    aws_secret_access_key = job_config.aws_secret_access_key
    emailaddr_to = job_config.email_to
    status = job_config.status
    client = boto3.client("ses", region_name="us-east-1", aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    alert_destination = {
        "ToAddresses": [emailaddr_to, ],
        "CcAddresses": [emailaddr_from, ]
    }
    alert_contents = {
        "Subject": {"Data": subject},
        "Body": {"Text": {"Data": body}}
    }
    try:
        client.send_email(Source=emailaddr_from, Destination=alert_destination, Message=alert_contents)
        print("[INFO] email notification sent emailaddr_to {} with status {}".format(emailaddr_to, status))
    except Exception as e:
        print("[WARNING] error sending email to {}: {}".format(emailaddr_to, e))
    return


def send_email_alert(job_config, emailaddr_from ="morf-alerts@umich.edu"):
    """
    Send email alert with status update email_to [email_to].
    You may need email_to verify the sender address by using verify_email_address()
        and clicking the link in the verification email sent to this address.
    :param job_config: MorfJobConfig object.
    :param emailaddr_from: email address to send alert from
    :return: None
    """
    subject = construct_message_subject(job_config)
    body = construct_message_body(job_config)
    ses_send_email(job_config, emailaddr_from, subject, body)
    return


def verify_email_address(aws_access_key_id, aws_secret_access_key, email = "morf-alerts@umich.edu"):
    """
    Send verification email to allow usage of email address via boto3; required for new/unverified addresses.
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param email:
    :return:
    """
    import boto.ses
    conn = boto.ses.connect_to_region("us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    conn.verify_email_address(email)
    return


def send_success_email(job_config, emailaddr_from ="morf-alerts@umich.edu"):
    """
    Send an email alert with an attachment.
    Modified substantially from:
    http://blog.vero4ka.info/blog/2016/10/26/how-to-send-an-email-with-attachment-via-amazon-ses-in-python/
    https://gist.github.com/yosemitebandit/2883593
    :param job_config: MorfJobConfig object.
    :param emailaddr_from: address to send email from (string).
    :return:
    """
    aws_access_key_id = job_config.aws_access_key_id
    aws_secret_access_key = job_config.aws_secret_access_key
    proc_data_bucket = job_config.proc_data_bucket
    job_id = job_config.job_id
    user_id = job_config.user_id
    emailaddr_to = job_config.email_to
    status = job_config.status
    job_config.update_mode("test") # need to set mode so that correct key path is used to fetch results
    results_file_name = "morf-results.csv"
    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    # fetch model evaluation results
    attachment_basename = generate_archive_filename(job_config, mode="evaluate", extension="csv")
    key = make_s3_key_path(job_config, filename=attachment_basename)
    attachment_filepath = download_from_s3(proc_data_bucket, key, s3)
    with open(attachment_filepath) as f:
        data = f.read()
    output = io.StringIO(data)
    # Build an email
    subject_text = construct_message_subject(job_config)
    msg = MIMEMultipart()
    msg["Subject"] = subject_text
    msg["From"] = emailaddr_from
    msg["To"] = emailaddr_to
    # What a recipient sees if they don't use an email reader
    msg.preamble = "Multipart message.\n"
    # the body
    body_text = construct_message_body(job_config)
    body = MIMEText(body_text)
    msg.attach(body)
    # The attachment
    part = MIMEApplication(output.getvalue())
    part.add_header("Content-Disposition", "attachment", filename=results_file_name)
    part.add_header("Content-Type", "application/vnd.ms-excel; charset=UTF-8")
    msg.attach(part)
    # Connect to Amazon SES
    ses = boto3.client(
        "ses",
        region_name="us-east-1",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    # And finally, send the email
    try:
        ses.send_raw_email(
            Source=emailaddr_from,
            Destinations=[emailaddr_to,
                          emailaddr_from],
            RawMessage={
                'Data': msg.as_string(),
            }
        )
        print("[INFO] email notification sent emailaddr_to {}".format(emailaddr_to))
    except Exception as e:
        print("[WARNING] error sending email to {}: {}".format(emailaddr_to, e))
    return


def send_queueing_alert(aws_access_key_id, aws_secret_access_key, emailaddr_to, emailaddr_from ="morf-alerts@umich.edu",
                        docs_url = "https://jpgard.github.io/morf/", emailaddr_info="morf-info@umich.edu",
                        status = "QUEUED"):
    """
    Send alert that job has been queued for execution.
    :return:
    """
    subject = "MORF job queued for execution"
    message_body = """

        This is an automated notification that your submission to MORF has been queued for execution.

        If you need support with MORF, check the MORF documentation at {} or contact the maintainers at {}.

        - The MORF team
        """.format(docs_url, emailaddr_info)
    ses_send_email(aws_access_key_id, aws_secret_access_key, emailaddr_to, emailaddr_from, subject, message_body, status)
    return
