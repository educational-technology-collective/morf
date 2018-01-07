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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=False, help="3-digit course run number")
    parser.add_argument("-m", "--mode", required=True, help="mode to run image in; {extract, train, test}")
    args = parser.parse_args()
    if args.mode == "extract":
        # this block expects individual session-level data mounted by extract_session() and outputs one CSV file per session in /output
        # set up the mysql database
        extract_coursera_sql_data(args.course, args.session)
        extract_features(course = args.course, session = args.session)
    elif args.mode == "train":
        # this block expects course-level data mounted by train_course() and outputs one model file per course in /output
        cmd = "Rscript /modeling/train_model_morf_mwe.R --course {}".format(args.course)
        subprocess.call(cmd, shell=True)
    elif args.mode == "test":
        # this block expects course-level data and models mounted by test_course() and outputs one csv of predictions per course in /output
        cmd = "Rscript /modeling/test_model_morf_mwe.R --course {}".format(args.course)
        subprocess.call(cmd, shell=True)
