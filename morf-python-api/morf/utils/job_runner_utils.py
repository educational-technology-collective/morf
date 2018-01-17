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
Utility functions specifically for running jobs in MORF API.
"""

import subprocess
import tempfile
import boto3
from morf.utils import *
from morf.utils.config import get_config_properties, combine_config_files, update_config_fields_in_section
from morf.utils.alerts import send_success_email, send_email_alert
from urllib.parse import urlparse
import os


def run_image(docker_url, user_id, job_id, mode, raw_data_bucket, course=None, session=None, level=None,
              label_type=None):
    """
    Run a docker image with the specified parameters.
    :param docker_url: URL for a built and compressed (.tar) docker image
    :param user_id: unique user id (string).
    :param job_id: unique job id (string).
    :param mode: mode to run image in; {extract, extract-holdout, train, test} (string).
    :param raw_data_bucket: raw data bucket; specify multiple buckets only if level == all'
    :param course: Coursera course slug or course shortname (string).
    :param session: 3-digit course session number (for trained model or extraction).
    :param level: level of aggregation of MORF API function; {session, course, all} (string).
    :param label_type: type of outcome label to use (required for model training and testing) (string).
    :return:
    """
    s3 = boto3.client("s3", aws_access_key_id=get_config_properties()["aws_access_key_id"],
                      aws_secret_access_key=get_config_properties()["aws_secret_access_key"])
    # create local directory for processing on this instance
    with tempfile.TemporaryDirectory(dir=get_config_properties()["local_working_directory"]) as working_dir:
        # download_docker_image(s3, working_dir, docker_url)
        fetch_file(s3, working_dir, docker_url, dest_filename="docker_image")
        input_dir, output_dir = initialize_input_output_dirs(working_dir)
        # fetch any data or models needed
        if "extract" in mode:  # download raw data
            initialize_raw_course_data(s3=s3, aws_access_key_id=get_config_properties()["aws_access_key_id"],
                                       aws_secret_access_key=get_config_properties()["aws_secret_access_key"],
                                       raw_data_bucket=raw_data_bucket, mode=mode, course=course, session=session,
                                       level=level, input_dir=input_dir)
        # fetch training/testing data and untar file for xing
        if mode in ["train", "test"]:
            if "xing" in job_id:
                download_train_test_data_xing(s3, input_dir, bucket=get_config_properties()["proc_data_bucket"],
                                              user_id=user_id, job_id=job_id, course=course)
            else:
                initialize_train_test_data(s3=s3, aws_access_key_id=get_config_properties()["aws_access_key_id"],
                                           aws_secret_access_key=get_config_properties()["aws_secret_access_key"],
                                           raw_data_bucket=raw_data_bucket,
                                           proc_data_bucket=get_config_properties()["proc_data_bucket"], mode=mode,
                                           level=level, user_id=user_id, job_id=job_id, label_type=label_type,
                                           course=course, session=session, input_dir=input_dir)
        if mode == "test":  # fetch models and untar
            download_models(bucket=get_config_properties()["proc_data_bucket"], user_id=user_id, s3=s3,
                            aws_access_key_id=get_config_properties()["aws_access_key_id"],
                            aws_secret_access_key=get_config_properties()["aws_secret_access_key"], job_id=job_id,
                            course=course, session=session, dest_dir=input_dir, level=level)
        # load the docker image and get its key
        local_docker_file_location = "{}/docker_image".format(working_dir)
        cmd = "{} load -i {};".format(get_config_properties()["docker_exec"], local_docker_file_location)
        print("[INFO] running: " + cmd)
        output = subprocess.run(cmd, stdout=subprocess.PIPE, shell = True)
        print(output.stdout.decode("utf-8"))
        image_uuid = output.stdout.decode("utf-8").split("sha256:")[-1].strip()
        # execute the image
        if mode == "extract-holdout":  # call docker image with mode == extract
            cmd = "{} run --network=\"none\" --rm=true --volume={}:/input --volume={}:/output {} --course {} --session {} --mode {};".format(
                get_config_properties()["docker_exec"], input_dir, output_dir, image_uuid, course, session, "extract")
        else:  # proceed as normal
            cmd = "{} run --network=\"none\" --rm=true --volume={}:/input --volume={}:/output {} --course {} --session {} --mode {};".format(
                get_config_properties()["docker_exec"], input_dir, output_dir, image_uuid, course, session, mode)
        print("[INFO] running: " + cmd)
        subprocess.call(cmd, shell=True)
        # cleanup
        cmd = "{} rmi --force {}".format(get_config_properties()["docker_exec"], image_uuid)
        print("[INFO] running: " + cmd)
        subprocess.call(cmd, shell=True)
        # archive and write output
        archive_file = make_output_archive_file(output_dir, mode=mode, user_id=user_id, job_id=job_id,
                                                course=course, session=session)
        move_results_to_destination(archive_file, bucket=get_config_properties()["proc_data_bucket"], user_id=user_id,
                                    job_id=job_id, mode=mode, course=course, session=session)
    return


def run_job(docker_url, mode, course, user, job_id, session, level, raw_data_bucket=None,
            label_type=None, raw_data_buckets=None):
    """
    Call job runner with correct parameters.
    :param docker_url: path to docker executable (string).
    :param mode: mode of job (string).
    :param course: course id (string); set as None if level == all.
    :param user: user name (string).
    :param job_id: user-specified job id (string).
    :param session: session number (string); set as none if level != session.
    :param level: one of {session, course, all}
    :param raw_data_bucket: name of bucket containing raw data.
    :param label_type: user-specified label type.
    :param raw_data_buckets: list of buckets (for use with level == all)
    :return: result of call to subprocess.call().
    """
    # todo: just set default values as none; no need for control flow below
    # todo: specify bucket here and make a required argument (currently run_image just defaults to using morf-michigan)
    # todo: different calls to run_image for each level are probably not necessary; all defaults are set to 'none'
    print("[INFO] running docker image {} user_id {} job_id {} course {} session {} mode {}"
          .format(docker_url, user, job_id, course, session, mode))
    if level == "all":
        run_image(docker_url, user, job_id, mode, raw_data_bucket=raw_data_buckets, level=level,
                  label_type=label_type)
    elif level == "course":
        run_image(docker_url, user, job_id, mode, raw_data_bucket, course=course, level=level, label_type=label_type)
    elif level == "session":
        run_image(docker_url, user, job_id, mode, raw_data_bucket, course=course, session=session, level=level,
                  label_type=label_type)
    return None


def run_morf_job(client_config_url, server_config_url, email_to = None, no_cache = False):
    """
    Wrapper function to run complete MORF job.
    :param client_config_url: url to client.config file; should be located on local machine.
    :param server_config_url: url (local or s3) to server.config file.
    :return:
    """
    controller_script_name = "controller.py"
    docker_image_name = "docker_image"
    server_config_path = urlparse(server_config_url).path
    # read server.config and get those properties
    server_config = get_config_properties(server_config_path)
    # create temporary directory in local_working_directory from server.config
    with tempfile.TemporaryDirectory(dir=server_config["local_working_directory"]) as working_dir:
        # save calling working directory; change directory into working_dir
        calling_dir = os.getcwd()
        os.chdir(working_dir)
        # download client.config into local_working_directory using AWS creds from server.config
        s3 = boto3.client("s3", aws_access_key_id=server_config["aws_access_key_id"],
                          aws_secret_access_key=server_config["aws_secret_access_key"])
        fetch_file(s3, working_dir, client_config_url)
        local_client_config_path = os.path.join(os.getcwd(), "client.config")
        combine_config_files(server_config_path, local_client_config_path)
        config = get_config_properties()
        if email_to: # if email_to was provided, this overrides in config file -- allows users to easily run mwe
            print("[INFO] email address from submission {} overriding email address in config file {}"
                  .format(email_to, config["email_to"]))
            config["email_to"] = email_to
            update_config_fields_in_section("client", email_to = email_to)
        cache_job_file_in_s3(s3, config["user_id"], config["job_id"], config["proc_data_bucket"])
        # from client.config, fetch and download the following: docker image, controller script
        try:
            fetch_file(s3, working_dir, config["docker_url"], dest_filename = docker_image_name)
            fetch_file(s3, working_dir, config["controller_url"], dest_filename = controller_script_name)
            if not no_cache: # cache job files in s3 unless no_cache parameter set to true
                cache_job_file_in_s3(s3, config["user_id"], config["job_id"], config["proc_data_bucket"],
                                     docker_image_name)
                cache_job_file_in_s3(s3, config["user_id"], config["job_id"], config["proc_data_bucket"],
                                     controller_script_name)
        except KeyError as e:
            cause = e.args[0]
            print("[Error]: field {} missing from client.config file.".format(cause))
            sys.exit(-1)
        # change working directory and run controller script with notifications for initialization and completion
        send_email_alert(config["aws_access_key_id"],
                         config["aws_secret_access_key"],
                         config["job_id"],
                         config["user_id"],
                         status = "INITIALIZED",
                         emailaddr_to=config["email_to"])
        subprocess.call("python3 {}".format(controller_script_name), shell = True)
        send_success_email(config["aws_access_key_id"],
                           config["aws_secret_access_key"],
                           config["proc_data_bucket"],
                           config["job_id"], config["user_id"], config["email_to"])
        return
