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
import os
import tarfile
import gzip
import shutil
import re
from feature_extraction.sql_utils import extract_grade_sql_data



def unarchive_file(src, dest):
    """
    Untar or un-gzip a file from src into dest. Supports file extensions: .zip, .tgz, .gz. Taken from MORF API.
    :param src: path to source file to unarchive (string).
    :param dest: directory to unarchive result into (string).
    :return: None
    """
    if src.endswith(".zip") or src.endswith(".tgz"):
        tar = tarfile.open(src)
        tar.extractall(dest)
        tar.close()
        outpath = os.path.join(dest, os.path.basename(src))
    elif src.endswith(".gz"):
        with gzip.open(src, "rb") as f_in:
            destfile = os.path.basename(src)[:-3] # source file without '.gz' extension
            destpath = os.path.join(dest, destfile)
            with open(destpath, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(src)
        outpath = destpath
    else:
        raise NotImplementedError("Passed in a file with an extension not supported by unarchive_file: {}".format(src))
    return outpath


def clean_filename(src):
    """
    Rename file, removing any non-alphanumeric characters.
    :param src: file to rename.
    :return: None
    """
    src_dir, src_file = os.path.split(src)
    clean_src_file = re.sub('[\(\)\s]', '', src_file)
    clean_src_path = os.path.join(src_dir, clean_src_file)
    try:
        os.rename(src, clean_src_path)
    except Exception as e:
        print("[ERROR] error renaming file: {}".format(e))
    return clean_src_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=True, help="3-digit course run number")
    parser.add_argument("-m", "--mode", required=True, help="mode to run image in; {extract, train, test}")
    args = parser.parse_args()
    if args.mode == "extract":
        # this block expects individual session-level data mounted by extract_session() and outputs one CSV file per session in /output
        # unzip files and clean names
        session_input_dir = os.path.join("/input", args.course, args.session)
        # fetch/create path names for various input and output files
        clickstream = [x for x in os.listdir(session_input_dir) if x.endswith("clickstream_export.gz")][0]
        clickstream_fp = os.path.join(session_input_dir, clickstream)
        clickstream_fp = unarchive_file(clickstream_fp, session_input_dir)
        clickstream_fp = clean_filename(clickstream_fp)
        outfile = "{}-{}-extract.csv".format(args.course, args.session)
        output_fp = os.path.join("/output", outfile)
        # run grade extraction script
        grade_file = extract_grade_sql_data(args.course, args.session)
        grade_fp = os.path.join(session_input_dir, grade_file)
        # compile the java code; ideally this should happen in dockerfile, not here!
        cmd = "javac -cp MORF1.4/jars/*: -d /MORF1.4/bin/ /MORF1.4/src/Extract.java"
        subprocess.call(cmd, shell=True)
        # run feature extraction
        os.chdir("/MORF1.4/bin/")
        cmd = "java -cp /MORF1.4/jars/*: Extract {} {} {}".format(clickstream_fp, grade_fp, output_fp)
        subprocess.call(cmd, shell=True)
