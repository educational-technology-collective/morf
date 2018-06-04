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
Utility functions for creating Digital Object Identifiers (doi) for executables submitted to MORF.
"""

import requests
import collections
from morf.utils.log import set_logger_handlers
import os
from morf.utils import fetch_file
import logging
import json

module_logger = logging.getLogger(__name__)


def create_empty_zenodo_upload(access_token):
    """
    Create an empty upload using Zenodo API.
    :param access_token: Zenodo access token.
    :return: requests.models.Response from Zenodo API
    """
    headers = {"Content-Type": "application/json"}
    r = requests.post('https://zenodo.org/api/deposit/depositions',
                      params={'access_token': access_token}, json={},
                      headers=headers)
    return r


def generate_zenodo_metadata(job_config, deposition_id):
    """
    Create metadata for a MORF job.
    :param job_config:
    :param deposition_id:
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    data = {
        'metadata': {
            'title': 'MORF job id {}'.format(job_config.morf_id),
            'upload_type': 'software',
            'description': 'Job files for job id {} from the MOOC Replication Framework'.format(job_config.morf_id),
            'creators': [{'name': '{}'.format(job_config.user_id), 'affiliation': 'None'}]
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.put('https://zenodo.org/api/deposit/depositions/%s' % deposition_id,
                     params = {'access_token': getattr(job_config, "zenodo_access_token")},
                     data = json.dumps(data), headers = headers)
    logger.info(r.json())
    return 


def publish_zenodo_deposition(job_config, deposition_id):
    logger = set_logger_handlers(module_logger, job_config)
    r = requests.post('https://zenodo.org/api/deposit/depositions/%s/actions/publish' % deposition_id,
                      params={'access_token': getattr(job_config, "zenodo_access_token")})
    logger.info(r.json())
    return


def upload_files_to_zenodo(job_config, upload_files, deposition_id = None, publish = True):
    """
    Upload each file in files to Zenodo, and publish the repo.
    :param deposition_id:
    :param files: a tuple of filenames to upload. These should be locally available.
    :param access_token:
    :return: deposition_id of Zenodo files
    """
    working_dir = os.getcwd()
    s3 = job_config.initialize_s3()
    logger = set_logger_handlers(module_logger, job_config)
    access_token = getattr(job_config, "zenodo_access_token")
    # check inputs
    assert isinstance(upload_files, collections.Iterable), "param 'files' must be an iterable"
    if not deposition_id: # create an empty upload and get its deposition id
        deposition_id = create_empty_zenodo_upload(access_token).json()['id']
    # upload each file
    for f in upload_files:
        fp = fetch_file(s3, working_dir, f, job_config=job_config)
        data = {'filename': fp}
        files = {'file': open(fp, 'rb')}
        r = requests.post('https://zenodo.org/api/deposit/depositions/%s/files' % deposition_id, params = {'access_token': access_token}, data = data, files = files)
        logger.info(r.json())
    # generate metadata for the zenodo repo and publish it
    generate_zenodo_metadata(job_config, deposition_id)
    if publish:
        publish_zenodo_deposition(job_config, deposition_id)
    return deposition_id

