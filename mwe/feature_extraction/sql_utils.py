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
import subprocess


def extract_coursera_sql_data(course, session, forum_comment_filename="forum_comments.csv", forum_post_filename="forum_posts.csv"):
    """
    Initializes the MySQL database. This assumes that MySQL is correctly setup in the docker container.
    :return:
    """
    course_session_dir = os.path.join(".", "input", course, session)
    mysql_to_csv_cmd = """ | tr '\t' ',' """  # string to properly format result of mysql query
    cwd = os.getcwd()
    hash_mapping_sql_dump = \
    [x for x in os.listdir(course_session_dir) if "hash_mapping" in x and session in x][0]
    forum_sql_dump = \
    [x for x in os.listdir(course_session_dir) if "anonymized_forum" in x and session in x][0]
    # start mysql server
    subprocess.call("service mysql start", shell=True)
    # command to create a database
    subprocess.call('''mysql -u root -proot -e "CREATE DATABASE course"''', shell=True)
    command = '''mysql -u root -proot course < {}'''.format(os.path.join(course_session_dir, forum_sql_dump))
    subprocess.call(command, shell=True)
    # command to load hash mapping
    command = '''mysql -u root -proot course < {}'''.format(os.path.join(course_session_dir, hash_mapping_sql_dump))
    subprocess.call(command, shell=True)
    # execute forum comment query and send to csv
    query = """SELECT thread_id , post_time , b.session_user_id FROM forum_comments as a LEFT JOIN hash_mapping as b ON a.user_id = b.user_id WHERE a.is_spam != 1 ORDER BY post_time;"""
    command = '''mysql -u root -proot course -e"{}"'''.format(query)
    command += """{} > {}""".format(mysql_to_csv_cmd, os.path.join(course_session_dir, forum_comment_filename))
    subprocess.call(command, shell=True)
    # execute forum post query and send to csv
    query = """SELECT id , thread_id , post_time , a.user_id , public_user_id , session_user_id , eventing_user_id FROM forum_posts as a LEFT JOIN hash_mapping as b ON a.user_id = b.user_id WHERE is_spam != 1 ORDER BY post_time;"""
    command = '''mysql -u root -proot course -e"{}"'''.format(query)
    command += """{} > {}""".format(mysql_to_csv_cmd, os.path.join(course_session_dir, forum_post_filename))
    subprocess.call(command, shell=True)
    return
