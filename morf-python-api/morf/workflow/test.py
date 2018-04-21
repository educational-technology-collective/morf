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
Testing functions for the MORF 2.0 API. For more information about the API, see the documentation.
"""

from morf.utils import *
from morf.utils.api_utils import *
from morf.utils.job_runner_utils import run_job
from morf.utils.alerts import send_email_alert
from morf.utils.config import get_config_properties, fetch_data_buckets_from_config, MorfJobConfig
import boto3
from multiprocessing import Pool

mode = "test"
# define module-level variables for config.properties
CONFIG_FILENAME = "config.properties"


def test_all(label_type):
    """
    test a single overall model using the entire dataset using the Docker image.
    :return:
    """
    level = "all"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    check_label_type(label_type)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    run_job(job_config, None, None, level, raw_data_buckets=job_config.raw_data_buckets)
    # fetch archived result file and push csv result back to s3, mimicking session- and course-level workflow
    result_file = collect_all_results(job_config)
    upload_key = make_s3_key_path(job_config, filename=generate_archive_filename(job_config, extension="csv"))
    upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def test_course(label_type, raw_data_dir="morf-data/", multithread=True):
    """
    tests one model per course using the Docker image.
    :param label_type:  label type provided by user.
    :raw_data_dir: path to directory in all data buckets where course-level directories are located; this should be uniform for every raw data bucket.
    :multithread: whether to run job in parallel (multithread = false can be useful for debugging).
    :return:
    """
    level = "course"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    check_label_type(label_type)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    ## for each bucket, call job_runner once per course with --mode=test and --level=course
    for raw_data_bucket in job_config.raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        courses = fetch_complete_courses(job_config, raw_data_bucket, raw_data_dir)
        if multithread:
            reslist = []
            with Pool(job_config.max_num_cores) as pool:
                for course in courses:
                    poolres = pool.apply_async(run_job, [job_config, course, None, level, raw_data_bucket, label_type])
                    reslist.append(poolres)
                pool.close()
                pool.join()
            for res in reslist:
                print(res.get())
        else:
            for course in courses:
                run_job(job_config, course, None, level, raw_data_bucket, label_type)
    result_file = collect_course_results(job_config)
    upload_key = make_s3_key_path(job_config, filename=generate_archive_filename(job_config, extension="csv"))
    upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return
