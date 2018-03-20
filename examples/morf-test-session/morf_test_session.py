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
Example of a minimum working example script for MORF 2.0.

Note that this script uses the --mode parameter from docker run to control the flow of extraction, training, and testing.

This script is structured to utilize the input and output contract of the extract_session() and train_session() functions from the MORF API.

This is a simple example of using session-level training; this workflow just uses the trained model from the most recent session for predicting on the test set. (for details, see )
"""

import argparse
import subprocess
from feature_extraction.morf_test_session_feature_extractor import main as extract_features
from feature_extraction.sql_utils import extract_coursera_sql_data
import os
import re
import pandas as pd
from multiprocessing import Pool


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='execute feature extraction, training, or testing.')
    parser.add_argument('--course', required=True, help='an s3 pointer to a course')
    parser.add_argument('--session', required=False, help='3-digit course run number')
    parser.add_argument('-m', '--mode', required=True, help='mode to run image in; {extract, train, test}')
    args = parser.parse_args()
    if args.mode == 'extract':
        # this block expects individual session-level data mounted by extract_session() and outputs one CSV file per session in /output
        # set up the mysql database
        extract_coursera_sql_data(args.course, args.session)
        extract_features(course_name = args.course, run_number = args.session)
    if args.mode == 'train':
        # this block expects session-level data mounted by train_session() and outputs one model file per session in /output
        cmd = "Rscript /modeling/train_model_morf_test_session.R --course {} --session {}".format(args.course, args.session)
        subprocess.call(cmd, shell=True)
    if args.mode == 'test':
        # this block expects session-level data and models mounted by test_course() and outputs one csv of predictions per course in /output, using only data from most recent iteration of course.
        cmd = "Rscript /modeling/test_model_morf_test_session.R --course {}".format(args.course)
        subprocess.call(cmd, shell=True)







