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

from morf.utils.config import MorfJobConfig
from morf.utils import fetch_courses, fetch_sessions, download_train_test_data, initialize_input_output_dirs, make_feature_csv_name, make_label_csv_name
from multiprocessing import Pool
import logging
import tempfile
import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold

module_logger = logging.getLogger(__name__)
CONFIG_FILENAME = "config.properties"

def create_session_folds(k = 5, multithread = True, raw_data_dir="morf-data/"):
    """
    From extract and extract-holdout data, create k randomized folds and archive results to s3.
    :param k: number of folds.
    :return:
    """
    job_config = MorfJobConfig(CONFIG_FILENAME)
    mode = "cv"
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
            for course in fetch_courses(job_config, raw_data_bucket):
                for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course, fetch_all_sessions=True):
                    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
                        input_dir, output_dir = initialize_input_output_dirs(working_dir)
                        # get the session data
                        download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type=None)
                        feature_file = os.path.join(input_dir, make_feature_csv_name(course, session))
                        label_file = os.path.join(input_dir, make_label_csv_name(course, session))
                        feat_df = pd.read_csv(feature_file, dtype=object)
                        label_df = pd.read_csv(label_file, dtype=object)
                        # TODO: StratifiedKFold splitting, write to file, and upload to s3
                        # read in session dat and split into folds, make sure to read each feature as object so as not to change type
                        for fold in range(1, k+1):
                            pass
        pool.close()
        pool.join()

