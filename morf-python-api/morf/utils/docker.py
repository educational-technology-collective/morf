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
Utility functions specifically for working with Docker images in MORF.
Note: functions for working with docker cloud are located in morf.utils.caching
"""


from morf.utils import *

module_logger = logging.getLogger(__name__)



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


def make_docker_image_name(job_config, course, session, mode, prefix="MORF"):
    """
    Create a uniqe name for the current job_config
    :param job_config:
    :return:
    """
    name = "{}-{}-{}-{}-{}".format(prefix, job_config.morf_id, mode, course, session)
    return name


def make_docker_run_command(job_config, docker_exec, input_dir, output_dir, image_uuid, course, session, mode, client_args = None):
    """
    Make docker run command, inserting MORF requirements along with any named arguments.
    :param client_args: doct of {argname, argvalue} pairs to add to command.
    :return:
    """
    image_name = make_docker_image_name(job_config, course, session, mode)
    cmd = "{} run --name {} --network=\"none\" --rm=true --volume={}:/input --volume={}:/output {} --course {} --session {} --mode {}".format(
        docker_exec, image_name, input_dir, output_dir, image_uuid, course, session, mode)
    if client_args:# add any additional client args to cmd
        for argname, argval in client_args.items():
            cmd += " --{} {}".format(argname, argval)
    return cmd


