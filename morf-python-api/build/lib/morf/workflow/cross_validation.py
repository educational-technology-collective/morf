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
Utility functions for performing cross-validation for model training/testing.
"""

from morf.utils.log import set_logger_handlers, execute_and_log_output
from morf.utils.docker import load_docker_image, make_docker_run_command
from morf.utils.config import MorfJobConfig
from morf.utils import fetch_complete_courses, fetch_sessions, download_train_test_data, initialize_input_output_dirs, make_feature_csv_name, make_label_csv_name, clear_s3_subdirectory, upload_file_to_s3, download_from_s3, initialize_labels, aggregate_session_input_data
from morf.utils.s3interface import make_s3_key_path
from morf.utils.api_utils import collect_course_cv_results
from multiprocessing import Pool
import logging
import tempfile
import pandas as pd
import os
import numpy as np
from sklearn.model_selection import StratifiedKFold


module_logger = logging.getLogger(__name__)
CONFIG_FILENAME = "config.properties"
mode = "cv"


def make_folds(job_config, raw_data_bucket, course, k, label_type, raw_data_dir="morf-data/"):
    """
    Utility function to be called by create_course_folds for creating the folds for a specific course.
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    user_id_col = "userID"
    label_col = "label_value"
    logger.info("creating cross-validation folds for course {}".format(course))
    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
        input_dir, output_dir = initialize_input_output_dirs(working_dir)
        # download data for each session
        for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course,
                                      fetch_all_sessions=True):
            # get the session feature and label data
            download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir,
                                     label_type=label_type)
        # merge features to ensure splits are correct
        feat_csv_path = aggregate_session_input_data("features", os.path.join(input_dir, course))
        label_csv_path = aggregate_session_input_data("labels", os.path.join(input_dir, course))
        feat_df = pd.read_csv(feat_csv_path, dtype=object)
        label_df = pd.read_csv(label_csv_path, dtype=object)
        feat_label_df = pd.merge(feat_df, label_df, on=user_id_col)
        if feat_df.shape[0] != label_df.shape[0]:
            logger.error(
                "number of observations in extracted features and labels do not match for course {}; features contains {} and labels contains {} observations".format(
                    course, feat_df.shape[0], label_df.shape[0]))
        # create the folds
        logger.info("creating cv splits with k = {} course {} session {}".format(k, course, session))
        skf = StratifiedKFold(n_splits=k, shuffle=True)
        folds = skf.split(np.zeros(feat_label_df.shape[0]), feat_label_df.label_value)
        for fold_num, train_test_indices in enumerate(folds, 1):  # write each fold train/test data to csv and push to s3
            train_index, test_index = train_test_indices
            train_df, test_df = feat_label_df.loc[train_index,].drop(label_col, axis=1), feat_label_df.loc[
                test_index,].drop(label_col, axis=1)
            train_df_name = os.path.join(working_dir, make_feature_csv_name(course, fold_num, "train"))
            test_df_name = os.path.join(working_dir, make_feature_csv_name(course, fold_num, "test"))
            train_df.to_csv(train_df_name, index=False)
            test_df.to_csv(test_df_name, index=False)
            # upload to s3
            try:
                train_key = make_s3_key_path(job_config, course, os.path.basename(train_df_name))
                upload_file_to_s3(train_df_name, job_config.proc_data_bucket, train_key, job_config, remove_on_success=True)
                test_key = make_s3_key_path(job_config, course, os.path.basename(test_df_name))
                upload_file_to_s3(test_df_name, job_config.proc_data_bucket, test_key, job_config, remove_on_success=True)
            except Exception as e:
                logger.warning("exception occurred while uploading cv results: {}".format(e))
    return


def create_course_folds(label_type, k = 5, multithread = True):
    """
    From extract and extract-holdout data, create k randomized folds, pooling data by course (across sessions) and archive results to s3.
    :param label_type: type of outcome label to use.
    :param k: number of folds.
    :param multithread: logical indicating whether multiple cores should be used (if available)
    :param raw_data_dir: name of subfolder in s3 buckets containing raw data.
    :return:
    """
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("creating cross-validation folds")
    for raw_data_bucket in job_config.raw_data_buckets:
        reslist = []
        with Pool(num_cores) as pool:
            for course in fetch_complete_courses(job_config, raw_data_bucket):
                poolres = pool.apply_async(make_folds, [job_config, raw_data_bucket, course, k, label_type])
                reslist.append(poolres)
            pool.close()
            pool.join()
        for res in reslist:
            logger.info(res.get())
    return


def create_session_folds(label_type, k = 5, multithread = True, raw_data_dir="morf-data/"):
    """
    From extract and extract-holdout data, create k randomized folds for each session and archive results to s3.
    :param label_type: type of outcome label to use.
    :param k: number of folds.
    :param multithread: logical indicating whether multiple cores should be used (if available)
    :param raw_data_dir: name of subfolder in s3 buckets containing raw data.
    :return:
    """
    user_id_col = "userID"
    label_col = "label_value"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("creating cross-validation folds")
    with Pool(num_cores) as pool:
        for raw_data_bucket in job_config.raw_data_buckets:
            for course in fetch_complete_courses(job_config, raw_data_bucket):
                for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course, fetch_all_sessions=True):
                    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
                        # todo: call make_folds() here via apply_async(); currently this is not parallelized!
                        input_dir, output_dir = initialize_input_output_dirs(working_dir)
                        # get the session feature and label data
                        download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type=label_type)
                        feature_file = os.path.join(input_dir, course, session, make_feature_csv_name(course, session))
                        label_file = os.path.join(input_dir, course, session, make_label_csv_name(course, session))
                        feat_df = pd.read_csv(feature_file, dtype=object)
                        label_df = pd.read_csv(label_file, dtype=object)
                        # merge features to ensure splits are correct
                        feat_label_df = pd.merge(feat_df, label_df, on=user_id_col)
                        assert feat_df.shape[0] == label_df.shape[0], "features and labels must contain same number of observations"
                        # create the folds
                        logger.info("creating cv splits with k = {} course {} session {}".format(k, course, session))
                        skf = StratifiedKFold(n_splits=k, shuffle=True)
                        folds = skf.split(np.zeros(feat_df.shape[0]), feat_label_df.label_value)
                        for fold_num, train_test_indices in enumerate(folds,1): # write each fold train/test data to csv and push to s3
                            train_index, test_index = train_test_indices
                            train_df, test_df = feat_label_df.loc[train_index,].drop(label_col, axis = 1), feat_label_df.loc[test_index,].drop(label_col, axis = 1)
                            train_df_name = os.path.join(working_dir, make_feature_csv_name(course, session, fold_num, "train"))
                            test_df_name = os.path.join(working_dir, make_feature_csv_name(course, session, fold_num, "test"))
                            train_df.to_csv(train_df_name, index = False)
                            test_df.to_csv(test_df_name, index=False)
                            # upload to s3
                            try:
                                train_key = make_s3_key_path(job_config, course, os.path.basename(train_df_name), session)
                                upload_file_to_s3(train_df_name, job_config.proc_data_bucket, train_key, job_config, remove_on_success=True)
                                test_key = make_s3_key_path(job_config, course, os.path.basename(test_df_name), session)
                                upload_file_to_s3(test_df_name, job_config.proc_data_bucket, test_key, job_config, remove_on_success=True)
                            except Exception as e:
                                logger.warning("exception occurred while uploading cv results: {}".format(e))
        pool.close()
        pool.join()
    return


def initialize_cv_labels(job_config, users, raw_data_bucket, course, label_type, input_dir, raw_data_dir, fold_num, type, level="course"):
    """

    :param job_config:
    :param train_users:
    :param test_users:
    :param raw_data_bucket:
    :param course:
    :param label_type:
    :param course_input_dir:
    :param raw_data_dir:
    :param fold_num:
    :param level:
    :return:
    """
    valid_types = ("train", "test")
    assert type in valid_types
    course_input_dir = os.path.join(input_dir, course)
    user_id_col = "userID"
    labels_path = initialize_labels(job_config, raw_data_bucket, course, None, label_type, course_input_dir, raw_data_dir, level="course")
    labels_df = pd.read_csv(labels_path, dtype=object)
    df_out = labels_df[labels_df[user_id_col].isin(users)]
    # test_labels_df = labels_df[labels_df[user_id_col].isin(test_users)]
    out_path = os.path.join(course_input_dir, make_label_csv_name(course, fold_num, type))
    # test_labels_path = os.path.join(course_input_dir, make_label_csv_name(course, fold_num, "test"))
    df_out.to_csv(out_path, index=False)
    # test_labels_df.to_csv(test_labels_path, index=False)
    os.remove(labels_path)
    return out_path


def execute_image_for_cv(job_config, raw_data_bucket, course, fold_num, docker_image_dir, label_type, raw_data_dir="morf-data/"):
    """

    :param job_config:
    :param raw_data_bucket:
    :param course:
    :param fold_num:
    :param docker_image_dir:
    :param label_type:
    :param raw_data_dir:
    :return:
    """
    user_id_col = "userID"
    logger = set_logger_handlers(module_logger, job_config)
    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
        input_dir, output_dir = initialize_input_output_dirs(working_dir)
        # get fold train data
        course_input_dir = os.path.join(input_dir, course)
        trainkey = make_s3_key_path(job_config, course, make_feature_csv_name(course, fold_num, "train"))
        train_data_path = download_from_s3(job_config.proc_data_bucket, trainkey, job_config.initialize_s3(), dir=course_input_dir, job_config=job_config)
        testkey = make_s3_key_path(job_config, course, make_feature_csv_name(course, fold_num, "test"))
        test_data_path = download_from_s3(job_config.proc_data_bucket, testkey, job_config.initialize_s3(), dir=course_input_dir, job_config=job_config)
        # get labels
        train_users = pd.read_csv(train_data_path)[user_id_col]
        train_labels_path = initialize_cv_labels(job_config, train_users, raw_data_bucket, course, label_type, input_dir, raw_data_dir, fold_num, "train", level="course")
        # run docker image with mode == cv
        image_uuid = load_docker_image(docker_image_dir, job_config, logger)
        cmd = make_docker_run_command(job_config, job_config.docker_exec, input_dir, output_dir, image_uuid, course, None, mode,
                                      job_config.client_args) + " --fold_num {}".format(fold_num)
        execute_and_log_output(cmd, logger)
        # upload results
        pred_csv = os.path.join(output_dir, "{}_{}_test.csv".format(course, fold_num))
        pred_key = make_s3_key_path(job_config, course, os.path.basename(pred_csv), mode="test")
        upload_file_to_s3(pred_csv, job_config.proc_data_bucket, pred_key, job_config, remove_on_success=True)
    return


def cross_validate_course(label_type, k=5, multithread=True):
    """
    Compute k-fold cross-validation across courses.
    :return:
    """
    # todo: call to create_course_folds() goes here
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    # clear previous test results
    clear_s3_subdirectory(job_config, mode="test")
    docker_image_dir = os.getcwd() # directory the function is called from; should contain docker image
    logger = set_logger_handlers(module_logger, job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("conducting cross validation")
    for raw_data_bucket in job_config.raw_data_buckets:
        reslist = []
        with Pool(num_cores) as pool:
            for course in fetch_complete_courses(job_config, raw_data_bucket):
                for fold_num in range(1, k + 1):
                    poolres = pool.apply_async(execute_image_for_cv, [job_config, raw_data_bucket, course, fold_num, docker_image_dir, label_type])
                    reslist.append(poolres)
            pool.close()
            pool.join()
        for res in reslist:
            logger.info(res.get())
    test_csv_fp = collect_course_cv_results(job_config)
    pred_key = make_s3_key_path(job_config, os.path.basename(test_csv_fp), mode="test")
    upload_file_to_s3(test_csv_fp, job_config.proc_data_bucket, pred_key, job_config, remove_on_success=True)
    return


def cross_validate_session(label_type, k = 5, multithread = True, raw_data_dir="morf-data/"):
    """
    Compute k-fold cross-validation across sessions.
    :return:
    """
    raise NotImplementedError # this is not implemented!
    # todo: call to create_session_folds() goes here
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    # clear any preexisting data for this user/job/mode
    # clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("conducting cross validation")
    with Pool(num_cores) as pool:
        for raw_data_bucket in job_config.raw_data_buckets:
            for course in fetch_complete_courses(job_config, raw_data_bucket):
                for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course, fetch_all_sessions=True):
                    for fold_num in range(1, k+1):
                        with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
                            # get fold train data
                            input_dir, output_dir = initialize_input_output_dirs(working_dir)
                            session_input_dir = os.path.join(input_dir, course, session)
                            session_output_dir = os.path.join(output_dir, course, session)
                            trainkey = make_s3_key_path(job_config, course, make_feature_csv_name(course, session, fold_num, "train"), session)
                            train_data_path = download_from_s3(job_config.proc_data_bucket, trainkey, job_config.initialize_s3(), dir=session_input_dir, job_config=job_config)
                            testkey = make_s3_key_path(job_config, course, make_feature_csv_name(course, session, fold_num, "test"), session)
                            test_data_path = download_from_s3(job_config.proc_data_bucket, testkey, job_config.initialize_s3(), dir=session_input_dir, job_config=job_config)
                            # get labels
                            initialize_labels(job_config, raw_data_bucket, course, session, label_type, session_input_dir, raw_data_dir)
                            # run docker image with mode == cv
                            #todo
                            # upload results
                            #todo
        pool.close()
        pool.join()
    return
