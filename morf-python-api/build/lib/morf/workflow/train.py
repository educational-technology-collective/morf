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
Training functions for the MORF 2.0 API. For more information about the API, see the documentation [HERE].
"""

from morf.utils import *
from morf.utils.api_utils import *
from morf.utils.job_runner_utils import run_image
from morf.utils.alerts import send_email_alert
from morf.utils.config import MorfJobConfig
from morf.utils.log import set_logger_handlers
from multiprocessing import Pool

mode = "train"
# define module-level variables for config.properties
CONFIG_FILENAME = "config.properties"
module_logger = logging.getLogger(__name__)


def train_all(label_type):
    """
    Train a single overall model using the entire dataset using the Docker image.
    :param label_type:  label type provided by user.
    :return: None
    """
    level = "all"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode("train")
    check_label_type(label_type)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    run_image(job_config, raw_data_bucket=job_config.raw_data_buckets, level=level, label_type=label_type)
    send_email_alert(job_config)
    return


def train_course(label_type, raw_data_dir="morf-data/", multithread=True):
    """
    Trains one model per course using the Docker image.
    :param label_type:  label type provided by user.
    :raw_data_dir: path to directory in all data buckets where course-level directories are located; this should be uniform for every raw data bucket.
    :multithread: whether to run job in parallel (multithread = false can be useful for debugging).
    :return: None
    """
    level = "course"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    check_label_type(label_type)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    # for each bucket, call job_runner once per course with --mode=train and --level=course
    for raw_data_bucket in job_config.raw_data_buckets:
        logger.info("processing bucket {}".format(raw_data_bucket))
        courses = fetch_complete_courses(job_config, raw_data_bucket, raw_data_dir)
        reslist = []
        with Pool(num_cores) as pool:
            for course in courses:
                poolres = pool.apply_async(run_image, [job_config, raw_data_bucket, course, None, level, label_type])
                reslist.append(poolres)
            pool.close()
            pool.join()
        for res in reslist:
            logger.info(res.get())
    send_email_alert(job_config)
    return


def train_session(label_type, raw_data_dir="morf-data/", multithread=True):
    """
    Train one model per session of the course using the Docker image.
    :param label_type:  label type provided by user.
    :raw_data_dir: path to directory in all data buckets where course-level directories are located; this should be uniform for every raw data bucket.
    :multithread: whether to run job in parallel (multithread = false can be useful for debugging).
    :return: None
    """
    level = "session"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    check_label_type(label_type)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    # for each bucket, call job_runner once per session with --mode=train and --level=session
    for raw_data_bucket in job_config.raw_data_buckets:
        logger.info("processing bucket {}".format(raw_data_bucket))
        courses = fetch_complete_courses(job_config, raw_data_bucket, raw_data_dir)
        reslist = []
        with Pool(num_cores) as pool:
            for course in courses:
                for session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course):
                    poolres = pool.apply_async(run_image, [job_config, raw_data_bucket, course, session, level, label_type])
                    reslist.append(poolres)
            pool.close()
            pool.join()
        for res in reslist:
            logger.info(res.get())
    send_email_alert(job_config)
    return
