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


if __name__ == "__main__":
    execution_output_file = "/input/prule_execute_output.csv" # name of outfile for "execute" step; passed as input to "combine" step
    final_output_file = "/output/output.csv"
    java_cmd_working_dir = "MORF-evaluate/bin/" # java scripts need to be run within this directory
    os.chdir(java_cmd_working_dir)
    input_dir = "/input/"
    prule_dir = "/prule"
    prule_filename = "morf-prule-sample.txt"
    prule_filepath = os.path.join(prule_dir, prule_filename)
    # run "execute" evaluation script
    cmd = "/usr/bin/java -cp /MORF-evaluate/jars/*: Execute {} {} {}".format(input_dir, prule_filepath, execution_output_file)
    subprocess.call(cmd, shell=True)
    # run "combine" evaluation script
    cmd = "java Combine {} {}".format(execution_output_file, final_output_file)
    subprocess.call(cmd, shell=True)
