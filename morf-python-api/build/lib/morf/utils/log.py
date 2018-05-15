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
import os


def set_logger_handlers(logger, job_config=None):
    """
    Sets filehandler and streamhandler so the logger goes to the correct output for job_config.
    :param logger:
    :param job_config:
    :return:
    """
    logger.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter, this is added to handlers later
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # set formatter and handler for console handler; this is used even if job_config is not provided
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # if job_config, create file handler for the job which logs even debug messages
    if job_config:
        job_log_filename = "{}.log".format(job_config.morf_id)
        fh = logging.FileHandler(os.path.join(job_config.logging_dir, job_log_filename))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
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


