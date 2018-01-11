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
Utility functions used throughout MORF API.
"""


from morf.utils import *
import tempfile
import pandas as pd


def fetch_result_csv_fp(dir):
    """
    Find result CSV in dir. Currently just finds the first non-system file CSV in dir, assuming only one exists; more sophisticated checks need to be added.
    :param dir: directory to search in.
    :return: path to csv.
    """
    # todo: check that there is only one CSV file
    # todo: check column names
    csv = [os.path.join(dir, x) for x in os.listdir(dir) if x.endswith(".csv") and not x.startswith(".")][0]
    return csv


def collect_session_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id, holdout = False, raw_data_dir = "morf-data/"):
    """
    Iterate through course- and session-level directories in bucket, download individual files from [mode], add column for course and session, and concatenate into single 'master' csv.
    :param s3: boto3.client object with appropriate access credentials.
    :param raw_data_buckets: list of buckets containing raw data; used to fetch course names from each bucket.
    :param raw_data_dir: path to directory in raw_data_bucket containing course-level directories.
    :param proc_data_bucket: bucket containing session-level archived results from [mode] jobs (i.e., session-level extracted features).
    :param mode: mode to collect results for, {extract, test}.
    :param holdout: flag; fetch holdout run only (boolean; default False).
    :return: path to csv.
    """
    # todo: possibly parallelize this using pool.map_async
    feat_df_list = list()
    for raw_data_bucket in raw_data_buckets:
        for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
            for run in fetch_sessions(s3, raw_data_bucket, raw_data_dir, course, fetch_holdout_session_only=holdout):
                with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
                    print("[INFO] fetching extraction results for course {} run {}".format(course, run))
                    try:
                        fetch_result_file(s3, proc_data_bucket, user_id=user_id, job_id=job_id, mode=mode, course=course, session= run, dir = working_dir)
                        csv = fetch_result_csv_fp(working_dir)
                        feat_df = pd.read_csv(csv, dtype=object)
                        feat_df['course'] = course
                        feat_df['session'] = run
                        feat_df_list.append(feat_df)
                    except:
                        print("[WARNING] no results found for course {} run {} mode {}".format(course, run, mode))
                        continue
    master_feat_df = pd.concat(feat_df_list)
    csv_fp = generate_archive_filename(user_id=user_id, job_id=job_id, mode=mode, extension='csv')
    master_feat_df.to_csv(csv_fp, index = False, header = True)
    return csv_fp


def collect_course_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id, raw_data_dir = "morf-data/"):
    """
    Iterate through course-level directories in bucket, download individual files from [mode], add column for course and session, and concatenate into single 'master' csv.
    :param s3: boto3.client object with appropriate access credentials.
    :param raw_data_buckets: list of buckets containing raw data; used to fetch course names from each bucket.
    :param raw_data_dir: path to directory in raw_data_bucket containing course-level directories.
    :param proc_data_bucket: bucket containing session-level archived results from [mode] jobs (i.e., session-level extracted features).
    :param mode: mode to collect results for, {extract, test}.
    :param holdout: flag; fetch holdout run only (boolean; default False).
    :return: path to csv.
    """
    feat_df_list = list()
    for raw_data_bucket in raw_data_buckets:
        for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
            with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
                print("[INFO] fetching extraction results for course {}".format(course))
                try:
                    fetch_result_file(s3, proc_data_bucket, user_id=user_id, job_id=job_id, mode=mode, course=course, dir=working_dir)
                    csv = fetch_result_csv_fp(working_dir)
                    feat_df = pd.read_csv(csv, dtype=object)
                    feat_df['course'] = course
                    feat_df_list.append(feat_df)
                except:
                    print("[WARNING] no results found for course {} mode {}".format(course, mode))
                    continue
    master_feat_df = pd.concat(feat_df_list)
    csv_fp = generate_archive_filename(user_id=user_id, job_id=job_id, mode=mode, extension='csv')
    master_feat_df.to_csv(csv_fp, index=False, header=True)
    return csv_fp


def collect_all_results(s3, proc_data_bucket, mode, user_id, job_id, raw_data_dir = "morf-data/"):
    """
    Pull results for all-level job and return path to csv.
    Similar wrapper to replicated workflow for collect_course_results and collect_session_results, but no iteration over courses/sessions required.
    :param s3:
    :param proc_data_bucket:
    :param mode:
    :param user_id:
    :param job_id:
    :param raw_data_dir:
    :return:
    """
    working_dir = os.getcwd()
    fetch_result_file(s3, proc_data_bucket, user_id=user_id, job_id=job_id, mode=mode, dir=working_dir)
    csv = fetch_result_csv_fp(working_dir)
    csv_fp = generate_archive_filename(user_id=user_id, job_id=job_id, mode=mode, extension="csv")
    shutil.move(csv, csv_fp)
    return csv_fp


def check_label_type(label_type, valid_labels = ["dropout", "dropout_current_week"]):
    """
    Helper function to check user-specified outcome label.
    :param label_type: label type provided by user.
    :param valid_labels: potential choices for valid labels available in MORF.
    :return: None.
    """
    if label_type not in valid_labels:
        exception_msg = "You specified an invalid label type; valid label types are:".format(", ".join(valid_labels))
        raise Exception(exception_msg)
    return
