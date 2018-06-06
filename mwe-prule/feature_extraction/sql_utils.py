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

DATABASE_NAME = "course"

def execute_mysql_query_into_csv(query, file, database_name=DATABASE_NAME):
    """
    Execute a mysql query into a file.
    :param query: valid mySQL query as string.
    :param file: csv filename to write to.
    :return: none
    """
    mysql_to_csv_cmd = """ | tr '\t' ',' """  # string to properly format result of mysql query
    command = '''mysql -u root -proot {} -e"{}"'''.format(database_name, query)
    command += """{} > {}""".format(mysql_to_csv_cmd, file)
    subprocess.call(command, shell=True)
    return


def load_mysql_dump(dumpfile, database_name=DATABASE_NAME):
    """
    Load a mySQL data dump into DATABASE_NAME.
    :param file: path to mysql database dump
    :return:
    """
    command = '''mysql -u root -proot {} < {}'''.format(database_name, dumpfile)
    subprocess.call(command, shell=True)
    return


def initialize_database(database_name=DATABASE_NAME):
    """
    Start mySQL service and initialize mySQL database with database_name.
    :param database_name: name of database.
    :return:
    """
    # start mysql server
    subprocess.call("service mysql start", shell=True)
    # create database
    subprocess.call('''mysql -u root -proot -e "CREATE DATABASE {}"'''.format(database_name), shell=True)
    return


def extract_grade_sql_data(course, session):
    """
    Initialize the mySQL database, load files, and execute queries to deposit csv files of data into /input/course/session directory.
    :param course: course name.
    :param session: session id.
    :param forum_comment_filename: name of csv file to write forum comments data to.
    :param forum_post_filename: name of csv file to write forum posts to.
    :return:
    """
    outfile = "{}-{}.csv".format(course, session)
    course_session_dir = os.path.join(".", "input", course, session)
    hash_mapping_sql_dump = \
    [x for x in os.listdir(course_session_dir) if "hash_mapping" in x and session in x][0]
    anon_general_sql_dump = \
    [x for x in os.listdir(course_session_dir) if "anonymized_general" in x and session in x][0]
    initialize_database()
    load_mysql_dump(os.path.join(course_session_dir, anon_general_sql_dump))
    load_mysql_dump(os.path.join(course_session_dir, hash_mapping_sql_dump))

    # execute forum comment query and send to csv
    query = "SELECT h.user_id, c.session_user_id, c.normal_grade FROM course_grades c LEFT JOIN hash_mapping h ON c.session_user_id = h.session_user_id;"
    execute_mysql_query_into_csv(query, os.path.join(course_session_dir, outfile))
    return outfile
