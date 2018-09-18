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

import numpy as np
import sklearn.metrics
from morf.utils.api_utils import *
from morf.utils.config import MorfJobConfig
from morf.utils.job_runner_utils import load_docker_image
from morf.utils.log import set_logger_handlers
from morf.utils.security import hash_df_column
from morf.utils.s3interface import make_s3_key_path

mode = "evaluate"
# define module-level variables for config.properties
CONFIG_FILENAME = "config.properties"
module_logger = logging.getLogger(__name__)


def check_dataframe_complete(df, job_config, columns):
    """
    Check columns for presence of NaN values; if any NaN values exist, throw message and raise exception.
    :param df: pd.DataFrame, containing columns.
    :param columns: columns to check for NaN values.
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    logger.info("[INFO] checking predictions")
    # filter to only include complete courses
    courses = [x[0] for x in fetch_all_complete_courses_and_sessions(job_config)]
    df_to_check = df[df.course.isin(courses)]
    null_counts = df_to_check.loc[:,columns].apply(lambda x: sum(x.isnull()), axis=0)
    if null_counts.sum() > 0:
        logger.error("Null values detected in the following columns: {} \n Did you include predicted probabilities and labels for all users?".format(null_counts.loc[null_counts > 0].index.tolist()))
        missing_courses = df_to_check[df_to_check.prob.isnull()]['course'].unique()
        logger.error("missing values detected in these courses: {}".format(missing_courses))
        raise
    else:
        return


def fetch_binary_classification_metrics(job_config, df, course, pred_prob_col = "prob", pred_col = "pred",
                                        label_col = "label_value", course_col = "course"):
    """
    Fetch set of binary classification metrics for df.
    :param job_config: MorfJobConfig object.
    :param df: pd.DataFrame of predictions; must include columns with names matching pred_prob_col, pred_col, and label_col.
    :param pred_prob_col: column of predicted probability of a positive class label. Should be in interval [0,1].
    :param pred_col: column of predicted class label. Should be in {0, 1}.
    :param label_col: column of true class label. Should be in {0, 1}
    :return: pd.DataFrame with dimension [1 x n_metrics].
    """
    logger = set_logger_handlers(module_logger, job_config)
    logger.info("fetching metrics for course {}".format(course))
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
        logger.warning("Only one class present in y_true for course {}. ROC AUC score, log_loss, precision, recall, F1 are undefined.".format(course))
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
    try:
        spec = cm[0,0] / float(cm[0,0] + cm[1,0])
    except Exception as e:
        print("[ERROR] error when computing specificity from confusion matrix: {}".format(e))
        print("confusion matrix is: {}".format(cm))
        spec = np.nan
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
                    course_col = "course", pred_cols = ("prob", "pred"),
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
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    course_data = []
    for raw_data_bucket in raw_data_buckets:
        pred_file = generate_archive_filename(job_config, mode="test", extension="csv")
        pred_key = "{}/{}/{}/{}".format(job_config.user_id, job_config.job_id, "test", pred_file)
        label_key = raw_data_dir + labels_file
        # download course prediction and label files, fetch classification metrics at course level
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
            download_from_s3(proc_data_bucket, pred_key, s3, working_dir, job_config=job_config)
            download_from_s3(raw_data_bucket, label_key, s3, working_dir, job_config=job_config)
            pred_df = pd.read_csv("/".join([working_dir, pred_file]))
            lab_df = pd.read_csv("/".join([working_dir, labels_file]), dtype=object)
            lab_df = lab_df[lab_df[label_col] == label_type].copy()
            pred_lab_df = pd.merge(lab_df, pred_df, how = "left", on = [user_col, course_col])
            check_dataframe_complete(pred_lab_df, job_config, columns = pred_cols)
            for course in fetch_complete_courses(job_config, data_bucket = raw_data_bucket, data_dir = raw_data_dir, n_train=1):
                course_metrics_df = fetch_binary_classification_metrics(job_config, pred_lab_df, course)
                course_data.append(course_metrics_df)
    master_metrics_df = pd.concat(course_data).reset_index().rename(columns={"index": course_col})
    csv_fp = generate_archive_filename(job_config, extension="csv")
    master_metrics_df[course_col] = hash_df_column(master_metrics_df[course_col], job_config.user_id, job_config.hash_secret)
    master_metrics_df.to_csv(csv_fp, index = False, header = True)
    upload_key = make_s3_key_path(job_config, mode = "test", filename=csv_fp)
    upload_file_to_s3(csv_fp, bucket=proc_data_bucket, key=upload_key)
    os.remove(csv_fp)
    return


def evaluate_cv_course(label_type, k=5, label_col = "label_type", raw_data_dir = "morf-data/",
                    course_col = "course", fold_col = "fold_num", pred_cols = ("prob", "pred"),
                    user_col = "userID"):
    """
    Fetch metrics by first averaging over folds within course, then returning results by course.
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
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    course_data = []
    for raw_data_bucket in raw_data_buckets:
        pred_file = generate_archive_filename(job_config, mode="test", extension="csv")
        pred_key = make_s3_key_path(job_config, pred_file, mode="test")
        # download course prediction and label files, fetch classification metrics at course level
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
            pred_csv = download_from_s3(proc_data_bucket, pred_key, s3, working_dir, job_config=job_config)
            job_config.update_mode("cv") # set mode to cv to fetch correct labels for sessions even if they are train/test sessions
            label_csv = initialize_labels(job_config, raw_data_bucket, None, None, label_type, working_dir, raw_data_dir, level="all")
            pred_df = pd.read_csv(pred_csv)
            lab_df = pd.read_csv(label_csv, dtype=object)
            pred_lab_df = pd.merge(lab_df, pred_df, how = "left", on = [user_col, course_col])
            check_dataframe_complete(pred_lab_df, job_config, columns = list(pred_cols))
            for course in fetch_complete_courses(job_config, data_bucket = raw_data_bucket, data_dir = raw_data_dir, n_train=1):
                fold_metrics_list = list()
                for fold_num in range(1, k+1):
                    fold_metrics_df = fetch_binary_classification_metrics(job_config, pred_lab_df[pred_lab_df[fold_col] == fold_num], course)
                    fold_metrics_list.append(fold_metrics_df)
                assert len(fold_metrics_list) == k, "something is wrong; number of folds doesn't match. Try running job again from scratch."
                course_metrics_df = pd.concat(fold_metrics_list).mean()
                course_metrics_df[course_col] = course
                course_data.append(course_metrics_df)
    job_config.update_mode(mode)
    master_metrics_df = pd.concat(course_data, axis = 1).T
    # reorder dataframe so course name is first
    cols = list(master_metrics_df)
    # move the column to head of list using index, pop and insert
    cols.insert(0, cols.pop(cols.index(course_col)))
    master_metrics_df = master_metrics_df.ix[:, cols]
    csv_fp = generate_archive_filename(job_config, extension="csv")
    master_metrics_df[course_col] = hash_df_column(master_metrics_df[course_col], job_config.user_id, job_config.hash_secret)
    master_metrics_df.to_csv(csv_fp, index = False, header = True)
    upload_key = make_s3_key_path(job_config, mode = "test", filename=csv_fp)
    upload_file_to_s3(csv_fp, bucket=proc_data_bucket, key=upload_key)
    os.remove(csv_fp)
    return


def evaluate_prule_session():
    """
    Perform statistical testing for prule analysis.
    :return: None
    """
    raw_data_dir = "morf-data/"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    raw_data_buckets = job_config.raw_data_buckets
    proc_data_bucket = job_config.proc_data_bucket
    prule_file = job_config.prule_url
    s3 = job_config.initialize_s3()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
        input_dir, output_dir = initialize_input_output_dirs(working_dir)
        # pull extraction results from every course into working_dir
        for raw_data_bucket in raw_data_buckets:
            for course in fetch_courses(job_config, raw_data_bucket):
                for session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course, fetch_all_sessions=True):
                    if session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course):
                        ## session is a non-holdout session
                        fetch_mode = "extract"
                    else:
                        fetch_mode = "extract-holdout"
                    feat_file = generate_archive_filename(job_config, course=course, session=session, mode=fetch_mode)
                    feat_key = make_s3_key_path(job_config, filename=feat_file, course=course, session=session, mode=fetch_mode)
                    feat_local_fp = download_from_s3(proc_data_bucket, feat_key, s3, input_dir, job_config=job_config)
                    unarchive_file(feat_local_fp, input_dir)
        docker_image_fp = urlparse(job_config.prule_evaluate_image).path
        docker_image_dir = os.path.dirname(docker_image_fp)
        docker_image_name = os.path.basename(docker_image_fp)
        image_uuid = load_docker_image(docker_image_dir, job_config, logger, image_name=docker_image_name)
        # create a directory for prule file and copy into it; this will be mounted to docker image
        prule_dir = os.path.join(working_dir, "prule")
        os.makedirs(prule_dir)
        shutil.copy(urlparse(prule_file).path, prule_dir)
        cmd = "{} run --network=\"none\" --rm=true --volume={}:/input --volume={}:/output --volume={}:/prule {} ".format(job_config.docker_exec, input_dir, output_dir, prule_dir, image_uuid)
        subprocess.call(cmd, shell=True)
        # rename result file and upload results to s3
        final_output_file = os.path.join(output_dir, "output.csv")
        final_output_archive_name = generate_archive_filename(job_config, extension="csv")
        final_output_archive_fp = os.path.join(output_dir, final_output_archive_name)
        os.rename(final_output_file, final_output_archive_fp)
        output_key = make_s3_key_path(job_config, filename = final_output_archive_name, mode = "test")
        upload_file_to_s3(final_output_archive_fp, proc_data_bucket, output_key, job_config, remove_on_success=True)
        return


