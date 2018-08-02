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
Functions for logging MORF activity.
"""

import json
import logging
import shlex
import subprocess
from logging.handlers import SMTPHandler
import os
from boto.ses.connection import SESConnection
from morf.utils.security import check_email_logging_authorized


class SESHandler(SMTPHandler):
    """ Send's an email using BOTO SES.
    """
    def __init__(self, mailhost, fromaddr, toaddrs, subject, job_config,
                 credentials=None, secure=None, timeout=5.0):
        """
        Initialize the handler (this is the default constructor with added handling of a MorfJobConfig input).
        """
        logging.Handler.__init__(self)
        if isinstance(mailhost, (list, tuple)):
            self.mailhost, self.mailport = mailhost
        else:
            self.mailhost, self.mailport = mailhost, None
        if isinstance(credentials, (list, tuple)):
            self.username, self.password = credentials
        else:
            self.username = None
        self.fromaddr = fromaddr
        if isinstance(toaddrs, str):
            toaddrs = [toaddrs]
        self.toaddrs = toaddrs
        self.subject = subject
        self.secure = secure
        self.timeout = timeout
        self.aws_access_key_id = job_config.aws_access_key_id
        self.aws_secret_access_key = job_config.aws_secret_access_key

    def emit(self,record):
        conn = SESConnection(self.aws_access_key_id, self.aws_secret_access_key)
        conn.send_email(self.fromaddr,self.subject,self.format(record),self.toaddrs)


def set_logger_handlers(logger, job_config=None, emailaddr_from ="morf-alerts@umich.edu"):
    """
    Sets filehandler and streamhandler so the logger goes to the correct output for job_config.
    :param logger:
    :param job_config:
    :return:
    """
    # create formatter, this is added to handlers later
    if sum([isinstance(x, logging.StreamHandler) for x in logger.handlers]) == 0: # no stream handler currently set; set one
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        # set formatter and handler for console handler; this is used even if job_config is not provided
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    # if job_config, create file handler for the job which logs even debug messages, if no file handler is currently set
    if job_config and (sum([isinstance(x, logging.FileHandler) for x in logger.handlers]) == 0):
        # logger = CustomAdapter(logger, {'morf_id': job_config.morf_id})
        job_log_filename = "{}.log".format(job_config.morf_id)
        fh = logging.FileHandler(os.path.join(job_config.logging_dir, job_log_filename))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - {} - {} - {} - %(levelname)s - %(message)s'.format(job_config.morf_id, job_config.user_id, job_config.job_id))
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    if job_config and (sum([isinstance(x, SMTPHandler) for x in logger.handlers]) == 0):
        if check_email_logging_authorized(job_config):
            email_subj = "MORF ID: {} exception".format(job_config.morf_id)
            mail_handler = SESHandler(mailhost="", fromaddr=emailaddr_from, toaddrs=job_config.email_to, subject=email_subj, job_config=job_config)
            mail_handler.setLevel(logging.ERROR)
            logger.addHandler(mail_handler)
    return logger


def log_job_params(logger, job_config):
    """
    Print job params to log file for debugging
    :param logger:
    :param job_config:
    :return:
    """
    logger.debug("MORF user_id {}".format(job_config.user_id))
    logger.debug("MORF job_id {}".format(job_config.job_id))
    logger.debug("MORF email_to {}".format(job_config.email_to))
    logger.debug("MORF docker_url {}".format(job_config.docker_url))
    logger.debug("MORF controller_url".format(job_config.controller_url))
    logger.debug("MORF job_config.__dict__ ".format(json.dumps(job_config.__dict__)))
    return


def initialize_logger(job_config, logger_name = "morf_api"):
    """
    Initialize the logger for a MORF job.
    :param job_config: MorfJobConfig object.
    :param logger_name: name of logger to use.
    :return: open coneection to logger object
    """
    logger = logging.getLogger(logger_name)
    logger = set_logger_handlers(logger, job_config)
    logger.info("logger initialization complete")
    log_job_params(logger, job_config)
    return logger


def execute_and_log_output(command, logger):
    """
    Execute command and log its output to logger.
    :param command:
    :param logger:
    :return:
    """
    logger.info("running: " + command)
    command_ary = shlex.split(command)
    p = subprocess.Popen(command_ary, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if stdout:
        logger.info(stdout)
    if stderr:
        logger.error(stderr)
    return