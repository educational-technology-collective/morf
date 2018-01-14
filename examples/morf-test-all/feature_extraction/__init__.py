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

import os
import pandas as pd
import shutil


def fetch_courses_and_sessions(dir="/input"):
    """
    Fetch list of (course_name, session) tuples in dir.
    :param dir: directory to search within; should have course/session subdirectories.
    :return: list of (course_name, session) tuples.
    """
    outlist = []
    courses = [x for x in os.listdir(dir) if not '.' in x]
    for course in courses:
        sessions = [x for x in os.listdir('/'.join([dir, course])) if not '.' in x ]
        for session in sessions:
            outlist.append((course, session))
    return outlist


def aggregate_output_csvs(output_dir = "/output", master_df_fp = "/output/feats.csv"):
    """

    :param output_dir: directory containing course-session-level data.
    :param master_df_fp: path to write master file of features.
    :return:
    """
    df_list = []
    for course, session in fetch_courses_and_sessions():
        csvname = "{}_{}.csv".format(course, session)
        fp = os.path.join(output_dir, csvname)
        df = pd.read_csv(fp)
        df["course"] = course
        df["session"] = session
        df_list.append(df)
        os.remove(fp)
    master_df = pd.concat(df_list)
    print("[INFO] writing features to {}".format(master_df_fp))
    master_df.to_csv(master_df_fp, index = False)
    return



