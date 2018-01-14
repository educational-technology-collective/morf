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

This script is structured to utilize the input and output contract of the extract_session() and train_course() functions from the MORF API.
"""

import argparse
import subprocess
from feature_extraction.mwe_feature_extractor import main as extract_features
from feature_extraction.sql_utils import extract_coursera_sql_data
import os
import re
import pandas as pd
from multiprocessing import Pool
from feature_extraction import fetch_courses_and_sessions, aggregate_output_csvs

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='execute feature extraction, training, or testing.')
    parser.add_argument('-m', '--mode', required=True, help='mode to run image in; {extract, train, test}')
    parser.add_argument('--course_id', required=False)
    parser.add_argument('--run_number', required=False)
    args = parser.parse_args()
    if args.mode == 'extract':
        # this block expects individual all data mounted by extract_all() and outputs one CSV file in /output
        # get list of courses from course-level directories in /input
        for c,s in fetch_courses_and_sessions():
        # set up the mysql database
            extract_coursera_sql_data(c, s)
            extract_features(course_name = c, run_number = s)
        aggregate_output_csvs()
    if args.mode == 'train':
        # this block expects all data mounted by train_all() and outputs one model in /output
        cmd = "Rscript /modeling/train_model_all.R "
        subprocess.call(cmd, shell=True)
    if args.mode == 'test':
        # this block expects course-level data and models mounted by test_course() and outputs one csv of predictions for all courses in /output
        cmd = "Rscript /modeling/test_model_all.R".format(args.course_id)
        subprocess.call(cmd, shell=True)







