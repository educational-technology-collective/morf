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
Evaluation utility functions for the MORF 2.0 API. For more information about the API, see the documentation [HERE].
"""

from morf.utils import *
from morf.utils.api_utils import *
from morf.utils.security import hash_df_column
from morf.utils.config import get_config_properties, fetch_data_buckets_from_config, MorfJobConfig
from morf.utils.alerts import send_email_alert
import tempfile
import pandas as pd
import numpy as np
import sklearn.metrics

mode = "evaluate"
# define module-level variables for config.properties
CONFIG_FILENAME = "config.properties"


def check_dataframe_complete(df, job_config, columns):
    """
    Check columns for presence of NaN values; if any NaN values exist, throw message and raise exception.
    :param df: pd.DataFrame, containing columns.
    :param columns: columns to check for NaN values.
    :return:
    """
    print("[INFO] checking predictions")
    # filter to only include complete courses
    courses = [x[0] for x in fetch_all_complete_courses_and_sessions(job_config)]
    df_to_check = df[df.course.isin(courses)]
    import ipdb;ipdb.set_trace()
    null_counts = df_to_check[columns].apply(lambda x: sum(x.isnull()), axis=0)
    if null_counts.sum() > 0:
        msg = "[ERROR] Null values detected in the following columns: {} \n Did you include predicted probabilities and labels for all users?".format(null_counts.loc[null_counts > 0].index.tolist())
        missing_courses = df_to_check[df_to_check.prob.isnull()]['course'].unique()
        print(msg)
        print("[ERROR] missing values detected in these courses: {}".format(missing_courses))
        raise
    else:
        return


def fetch_binary_classification_metrics(df, course, pred_prob_col = "prob", pred_col = "pred", 
                                        label_col = "label_value", course_col = "course"):
    """
    Fetch set of binary classification metrics for df.
    :param df: pd.DataFrame of predictions; must include columns with names matching pred_prob_col, pred_col, and label_col.
    :param pred_prob_col: column of predicted probability of a positive class label. Should be in interval [0,1].
    :param pred_col: column of predicted class label. Should be in {0, 1}.
    :param label_col: column of true class label. Should be in {0, 1}
    :return: pd.DataFrame with dimension [1 x n_metrics].
    """
    print("[INFO] fetching metrics for course {}".format(course))
    df = df[df[course_col] == course]
    metrics = {}
    y_pred = df[pred_col].values.astype(float)
    y_true = df[label_col].values.astype(float)
    y_score = df[pred_prob_col].values
    metrics["accuracy"] = sklearn.metrics.accuracy_score(y_true, y_pred)
    try:
        metrics["auc"] = sklearn.metrics.roc_auc_score(y_true, y_score)
        metrics["log_loss"] = sklearn.metrics.log_loss(y_true, y_score)
        metrics["precision"] = sklearn.metrics.precision_score(y_true, y_pred)  #
        metrics["recall"] = sklearn.metrics.recall_score(y_true, y_pred)  # true positive rate, sensitivity
        metrics["f1_score"] = sklearn.metrics.f1_score(y_true, y_pred)
    except ValueError:
        print("[WARNING]Only one class present in y_true for course {}. ROC AUC score, log_loss, precision, recall, F1 are undefined.".format(course))
        metrics["auc"] = np.nan
        metrics["log_loss"] = np.nan
        metrics["precision"] = np.nan
        metrics["recall"] = np.nan
        metrics["f1_score"] = np.nan
    metrics["cohen_kappa_score"] = sklearn.metrics.cohen_kappa_score(y_true, y_pred)
    metrics["N"] = df.shape[0]
    metrics["N_n"] = df[label_col].value_counts().get(0,0)
    metrics["N_p"] = df[label_col].value_counts().get(1,0)
    cm = sklearn.metrics.confusion_matrix(y_true, y_pred)
    spec = cm[0,0] / float(cm[0,0] + cm[1,0])
    metrics["specificity"] = spec
    metrics_df = pd.DataFrame(metrics, index = [course])
    return metrics_df


def evaluate_all():
    """
    Fetch metrics overall.
    :return:
    """
    # TODO
    return


def evaluate_course(label_type, label_col = "label_type", raw_data_dir = "morf-data/",
                    course_col = "course", pred_cols = ["prob", "pred"],
                    user_col = "userID", labels_file = "labels-test.csv"):
    """
    Fetch metrics by course.
    :param label_type: label type defined by user.
    :param label_col: column containing labels.
    :param raw_data_bucket: bucket containing raw data; used to fetch course names.
    :param raw_data_dir: path to directory in raw_data_bucket containing course-level directories.
    :param proc_data_bucket: bucket containing session-level archived results from [mode] jobs (i.e., session-level extracted features).
    :param course_col: column containing course identifier.
    :param pred_cols: user-supplied prediction columns; these columns will be checked for missing values and to ensure they contain values for every user in the course.
    :param user_col: column containing user ID for predictions.
    :param labels_file: name of csv file containing labels.
    :return: None.
    """
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    check_label_type(label_type)
    raw_data_buckets = job_config.raw_data_buckets
    proc_data_bucket = job_config.proc_data_bucket
    s3 = job_config.initialize_s3()
    course_data = []
    for raw_data_bucket in raw_data_buckets:
        pred_file = generate_archive_filename(job_config, mode="test", extension="csv")
        pred_key = "{}/{}/{}/{}".format(job_config.user_id, job_config.job_id, "test", pred_file)
        label_key = raw_data_dir + labels_file
        # download course prediction and label files, fetch classification metrics at course level
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
            download_from_s3(proc_data_bucket, pred_key, s3, working_dir)
            download_from_s3(raw_data_bucket, label_key, s3, working_dir)
            pred_df = pd.read_csv("/".join([working_dir, pred_file]))
            lab_df = pd.read_csv("/".join([working_dir, labels_file]))
            lab_df = lab_df[lab_df[label_col] == label_type].copy()
            pred_lab_df = pd.merge(lab_df, pred_df, how = "left", on = [user_col, course_col])
            check_dataframe_complete(pred_lab_df, job_config, columns = pred_cols)
            for course in fetch_complete_courses(job_config, data_bucket = raw_data_bucket, data_dir = raw_data_dir, n_train=1):
                course_metrics_df = fetch_binary_classification_metrics(pred_lab_df, course)
                course_data.append(course_metrics_df)
    master_metrics_df = pd.concat(course_data).reset_index().rename(columns={"index": course_col})
    csv_fp = generate_archive_filename(job_config, extension="csv")
    master_metrics_df[course_col] = hash_df_column(master_metrics_df[course_col], job_config.user_id, job_config.hash_secret)
    master_metrics_df.to_csv(csv_fp, index = False, header = True)
    upload_key = make_s3_key_path(job_config, mode = "test", filename=csv_fp)
    upload_file_to_s3(csv_fp, bucket=proc_data_bucket, key=upload_key)
    os.remove(csv_fp)
    return
