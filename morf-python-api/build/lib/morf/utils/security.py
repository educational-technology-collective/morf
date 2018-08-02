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

import hashlib
import pandas as pd


def hash_df_column(column, user_id, hash_secret):
    """
    Create a sha1-hashed version of a Series.
    :param column: Series (pandas or numpy) of characters to be hashed
    :param user_id: unique user id (string).
    :param hash_secret: secret hash (string).
    :return:
    """
    column_hashed = column.apply(lambda x: hashlib.sha1("{}{}{}".format(x, user_id, hash_secret).encode("utf-8")).hexdigest())
    return column_hashed


def generate_md5(fname):
    """
    Generates an md5 for a file. Based on https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
    :param fname: file name.
    :return:
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def check_email_logging_authorized(job_config, auth_colname = "email_logging_authorized"):
    """
    Check whether a user is permitted to receive email logging updates.
    :param job_config: MorfJobConfig object.
    :param auth_colname: name of column in authorization table that is used to check auth values.
    :return: Boolean indicating whether user is permitted to receive email logging.
    """
    auth_dict = {"T": True, "F": False} # mapping of plain-text boolean values used in table to Python logicals
    access_table = pd.read_csv(job_config.access_table_url, index_col=0)
    try:
        email_logging_auth = access_table.loc[job_config.email_to, auth_colname]
        authorized = auth_dict.get(email_logging_auth)
    except:
        authorized = False
    return authorized
