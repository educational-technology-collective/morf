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

import shlex
import tempfile
from morf.utils import *
from morf.utils.alerts import send_success_email, send_email_alert
from morf.utils.caching import update_morf_job_cache
from urllib.parse import urlparse
import os
from morf.utils.log import set_logger_handlers


module_logger = logging.getLogger(__name__)


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


def load_docker_image(dir, job_config, logger, image_name = "docker_image"):
    """
    Load docker_image from dir, writing output to logger.
    :param dir: Path to directory containing image_name.
    :param job_config: MorfJobConfig object.
    :param logger: Logger to log output to.
    :param image_name: base name of docker image.
    :return: SHA256 or tag name of loaded docker image
    """
    # load the docker image and get its key
    local_docker_file_location = os.path.join(dir, image_name)
    cmd = "{} load -i {};".format(job_config.docker_exec, local_docker_file_location)
    logger.info("running: " + cmd)
    output = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    logger.info(output.stdout.decode("utf-8"))
    load_output = output.stdout.decode("utf-8")
    if "sha256:" in load_output:
        image_uuid = output.stdout.decode("utf-8").split("sha256:")[-1].strip()
    else:  # image is tagged
        image_uuid = load_output.split()[-1].strip()
    return image_uuid


def run_image(job_config, raw_data_bucket, course=None, session=None, level=None, label_type=None):
    """
    Run a docker image with the specified parameters.
    :param docker_url: URL for a built and compressed (.tar) docker image
    :param user_id: unique user id (string).
    :param job_id: unique job id (string).
    :param mode: mode to run image in; {extract, extract-holdout, train, test} (string).
    :param raw_data_bucket: raw data bucket; specify multiple buckets only if level == all.
    :param course: Coursera course slug or course shortname (string).
    :param session: 3-digit course session number (for trained model or extraction).
    :param level: level of aggregation of MORF API function; {session, course, all} (string).
    :param label_type: type of outcome label to use (required for model training and testing) (string).
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    docker_url = job_config.docker_url
    mode = job_config.mode
    s3 = job_config.initialize_s3()
    docker_exec = job_config.docker_exec
    # create local directory for processing on this instance
    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
        try:
            fetch_file(s3, working_dir, docker_url, dest_filename="docker_image")
        except Exception as e:
            logger.error("[ERROR] Error downloading file {} to {}".format(docker_url, working_dir))
        input_dir, output_dir = initialize_input_output_dirs(working_dir)
        # fetch any data or models needed
        if "extract" in mode:  # download raw data
            initialize_raw_course_data(job_config,
                                       raw_data_bucket=raw_data_bucket, mode=mode, course=course,
                                       session=session, level=level, input_dir=input_dir)
        # fetch training/testing data
        if mode in ["train", "test"]:
            initialize_train_test_data(job_config, raw_data_bucket=raw_data_bucket, level=level,
                                       label_type=label_type, course=course, session=session,
                                       input_dir=input_dir)
        if mode == "test":  # fetch models and untar
            download_models(job_config, course=course, session=session, dest_dir=input_dir, level=level)
        image_uuid = load_docker_image(dir=working_dir, job_config=job_config, logger=logger)
        # build docker run command and execute the image
        if mode == "extract-holdout":  # call docker image with mode == extract
            cmd = "{} run --network=\"none\" --rm=true --volume={}:/input --volume={}:/output {} --course {} --session {} --mode {}".format(
                docker_exec, input_dir, output_dir, image_uuid, course, session, "extract")
        else:  # proceed as normal
            cmd = "{} run --network=\"none\" --rm=true --volume={}:/input --volume={}:/output {} --course {} --session {} --mode {}".format(
                docker_exec, input_dir, output_dir, image_uuid, course, session, mode)
        # add any additional client args to cmd
        if job_config.client_args:
            for argname, argval in job_config.client_args.items():
                cmd += " --{} {}".format(argname, argval)
        execute_and_log_output(cmd, logger)
        # cleanup
        execute_and_log_output("{} rmi --force {}".format(docker_exec, image_uuid), logger)
        # archive and write output
        archive_file = make_output_archive_file(output_dir, job_config, course = course, session = session)
        move_results_to_destination(archive_file, job_config, course = course, session = session)
    return


def run_morf_job(job_config, no_cache = False):
    """
    Wrapper function to run complete MORF job.
    :param client_config_url: url to client.config file.
    :param server_config_url: url to server.config file.
    :return:
    """
    combined_config_filename = "config.properties"
    logger = set_logger_handlers(module_logger, job_config)
    logger.info("running job id: {}".format(job_config.morf_id))
    controller_script_name = "controller.py"
    docker_image_name = "docker_image"
    s3 = job_config.initialize_s3()
    # create temporary directory in local_working_directory from server.config
    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
        # copy config file into new directory
        shutil.copy(combined_config_filename, working_dir)
        os.chdir(working_dir)
        # from job_config, fetch and download the following: docker image, controller script, cached config file
        update_morf_job_cache(job_config)
        # from client.config, fetch and download the following: docker image, controller script
        try:
            fetch_file(s3, working_dir, job_config.docker_url, dest_filename=docker_image_name, job_config=job_config)
            fetch_file(s3, working_dir, job_config.controller_url, dest_filename=controller_script_name, job_config=job_config)
            if not no_cache: # cache job files in s3 unless no_cache parameter set to true
                cache_job_file_in_s3(job_config, filename = docker_image_name)
                cache_job_file_in_s3(job_config, filename = controller_script_name)
        except KeyError as e:
            cause = e.args[0]
            logger.error("[Error]: field {} missing from client.config file.".format(cause))
            sys.exit(-1)
        # change working directory and run controller script with notifications for initialization and completion
        job_config.update_status("INITIALIZED")
        send_email_alert(job_config)
        subprocess.call("python3 {}".format(controller_script_name), shell = True)
        job_config.update_status("SUCCESS")
        send_success_email(job_config)
        return
