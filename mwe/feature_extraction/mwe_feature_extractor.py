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
Takes gzipped Coursera clickstream/log files as input and returns a single feature set in /output/ directory.
This is a minimum working example for MORF 2.0.
"""

from argparse import ArgumentParser
from bisect import bisect_left
import csv
from datetime import datetime, timedelta
import gzip
import itertools
from json import loads
from math import ceil
from os import path, makedirs, listdir
import re
from collections import defaultdict, Counter

import pandas as pd

MILLISECONDS_IN_SECOND = 1000


def fetch_start_end_date(course_name, run, date_csv = "coursera_course_dates.csv"):
    """
    Fetch course start end end date (so user does not have to specify them directly).
    :param course_name: Short name of course.
    :param run: run number
    :param date_csv: Path to csv of course start/end dates.
    :return: tuple of datetime objects (course_start, course_end)
    """
    full_course_name = "{0}-{1}".format(course_name, run)
    date_df = pd.read_csv(date_csv, usecols=[0, 2, 3]).set_index("course")
    course_start = datetime.strptime(date_df.loc[full_course_name].start_date, "%m/%d/%y")
    course_end = datetime.strptime(date_df.loc[full_course_name].end_date, "%m/%d/%y")
    return (course_start, course_end)


def course_len(course_start, course_end):
    """
    Return the duration of a course, in number of whole weeks.
    Note: Final week may be less than 7 days, depending on course start and end dates.
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: integer of course duration in number of weeks (rounded up if necessary)
    """
    course_start, course_end = course_start, course_end
    n_days = (course_end - course_start).days
    n_weeks = ceil(n_days / 7)
    return n_weeks


def timestamp_week(timestamp, course_start, course_end):
    """
    Get (zero-indexed) week number for a given timestamp.
    :param timestamp: UTC timestamp, in seconds.
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: integer week number of timestamp. If week not in range of course dates provided, return None.
    """
    timestamp = datetime.fromtimestamp(timestamp / MILLISECONDS_IN_SECOND)
    n_weeks = course_len(course_start, course_end)
    week_starts = [course_start + timedelta(days=x) for x in range(0, n_weeks * 7, 7)]
    week_number = bisect_left(week_starts, timestamp) - 1
    if week_number >= 0 and week_number <= n_weeks:
        return week_number
    else: # invalid week number; either before official course start or after official course end
        return None


def extract_users(coursera_clickstream_file, course_start, course_end):
    """
    Assemble list of all users in clickstream.
    :param coursera_clickstream_file: gzipped Coursera clickstream file
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: Python set of all unique user IDs that registered any activity in clickstream log
    """
    users = set()
    linecount = 0 # indexes line number
    with gzip.open(coursera_clickstream_file, "r") as f:
        for line in f:
            try:
                log_entry = loads(line.decode("utf-8"))
                user = log_entry.get("username")
                users.add(user)
            except ValueError as e1:
                print("Warning: invalid log line {0}: {1}".format(linecount, e1))
            except Exception as e:
                print("Warning: invalid log line {0}: {1}\n{2}".format(linecount, e, line))
            linecount += 1
    return users


def extract_forum_posts(forumposts, forumcomments, users, course_start, course_end):
    """
    Extract counts of forum posts and comments by user/week.
    This script treats forum posts and comments as identical actions.
    See forum_post_sql_query.txt for sample SQL query used to generate a file with this format.
    Note: this function could be modified to extract data directly
        from database using the SQL queries included in /sampledata.
    :param forumposts: csv generated by forum_post_sql_query
        columns: id, thread_id, post_time, user_id,
        public_user_id, session_user_id, eventing_user_id; see ./sampledata for example
    :param forumcomments: csv generated by forum_comment_sql_query
        columns: thread_id, post_time, session_user_id; see ./sampledata for example
    :param users: list of all user IDs, from extract_users(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: a pandas.DataFrame with columns userID, week, forum_posts
    """
    n_weeks = course_len(course_start, course_end)
    output = {user: {n: 0 for n in range(n_weeks + 1)} for user in users}
    with open(forumposts) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = int(row.get("post_time")) * MILLISECONDS_IN_SECOND
            week = timestamp_week(timestamp, course_start, course_end)
            if week:
                suid = row.get("session_user_id")
                try: # increment weekly post count for that user
                    output[suid][week] += 1
                except KeyError: # user posted in forums but registered no clickstream activity; create entry for user
                    print("Warning: user {0} posted in forum but not in course users list.".format(suid))
                    output[suid] = {n: 0 for n in range(n_weeks + 1)}
                    output[suid][week] = 1
    with open(forumcomments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = int(row.get("post_time")) * MILLISECONDS_IN_SECOND
            week = timestamp_week(timestamp, course_start, course_end)
            if week:
                suid = row.get("session_user_id")
                try:
                    output[suid][week] += 1
                except KeyError: # this means user posted in forums but somehow registered no clickstream activity
                    print("Warning: user {0} commented in forum but not in course users list.".format(suid))
                    output[suid] = {n: 0 for n in range(n_weeks + 1)}
                    output[suid][week] = 1
    post_list = [(user, week, posts)
                 for user, user_data in output.items()
                 for week, posts in user_data.items()]
    df_post = pd.DataFrame(post_list,
                           columns=["userID", "week", "forum_posts"])\
                           .set_index("userID")
    return df_post


def generate_appended_xing_csv(df_in, week):
    """
    Create appended feature set for week from df_in.
    :param df_in: Full pandas.DataFrame of userID, week, and additional features.
    :param week: Week to create appended feature set for (starting at zero, inclusive)
    :return: pandas.DataFrame of appended ('wide') features for weeks in interval [0, week].
    """
    for i in range(0, week+1): # append data from weeks 0-current week
        df_to_append = df_in[df_in.week == i].drop("week", axis=1)
        df_to_append = df_to_append\
            .rename(columns = lambda x: "week_{0}_{1}".format(str(i), str(x)))\
            .reset_index()
        if i ==0: #nothing to merge on yet; initialize df_app using week 0 features
            df_app = df_to_append.set_index("userID")
        else: #append features by merging to current feature set
            df_app = df_app.reset_index()\
            .merge(df_to_append)\
            .set_index("userID")
    return df_app


def generate_weekly_csv(df_in, out_dir, i):
    """
    Create a series of csv files containing all entries for each week in df_in
    :param df_in: pandas.DataFrame of weekly features to write output for
    :param i: week to generate features for
    :return: Nothing returned; writes csv files to out_dir.
    """
    if not path.exists(out_dir):
        makedirs(out_dir)
    df_out = df_in.copy()
    wk_appended_df = generate_appended_xing_csv(df_in, i)
    destfile = "{}/morf_mwe_feats.csv".format(out_dir,i)
    wk_appended_df.to_csv(destfile)


def extract_features(forumfile, commentfile, users, course_start, course_end):
    """
    Extract features from forum and comment file.
    :param forumfile: csv generated by forum_post_sql_query
        columns: id, thread_id, post_time, user_id,
        public_user_id, session_user_id, eventing_user_id; see ./sampledata for example
    :param commentfile: csv generated by forum_comment_sql_query
        columns: thread_id, post_time, session_user_id; see ./sampledata for example
    :param users: list of all user IDs, from extract_users(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: pandas.DataFrame of features by user id and week.
    """
    print("Extracting forum posts...")
    forumposts = extract_forum_posts(forumfile, commentfile, users, course_start, course_end)
    features_df = forumposts.reset_index().set_index("userID")
    return features_df


def main(course, session, n_feature_weeks = 4, out_dir ="/output"):
    """
    Extract counts of forum posts by week and write to /output.
    :param course: Coursera course slug (string).
    :param session: 3-digit run number (string).
    :param n_feature_weeks: number of weeks of features to consider (int).
    :return: None; writes output for weekly CSV file in /output.
    """
    session_dir = "/input/{0}/{1}/".format(course, session)
    clickstream = [x for x in listdir(session_dir) if x.endswith("clickstream_export.gz")][0]
    clickstream_fp = "{0}{1}".format(session_dir, clickstream)
    forumfile = "{0}forum_posts.csv".format(session_dir)
    commentfile = "{0}forum_comments.csv".format(session_dir)
    datefile = "{0}coursera_course_dates.csv".format(session_dir)
    course_start, course_end = fetch_start_end_date(course, session, datefile)
    # build features
    print("Extracting users...")
    users = extract_users(clickstream_fp, course_start, course_end)
    print("Complete. Extracting features...")
    feats_df = extract_features(forumfile, commentfile, users, course_start, course_end)
    # write output
    generate_weekly_csv(feats_df, out_dir=out_dir, i = n_feature_weeks)
    print("Output written to {0}".format(out_dir))


if __name__ == "__main__":
    # build parser
    parser = ArgumentParser(description="Create features from Coursera clickstream file.")
    parser.add_argument("-n", "--course_name",
                        metavar="course short name [must match name in coursera_course_dates.csv; ex. 'introfinance'",
                        type=str,
                        required=True)
    parser.add_argument("-r", "--run_number", metavar="3-digit run number", type=str, required=True)
    # collect input from parser and assign variables
    args = parser.parse_args()
    main(course=args.course_name, session=args.run_number)
