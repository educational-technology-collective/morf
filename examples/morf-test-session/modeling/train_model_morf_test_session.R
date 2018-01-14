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

## script to train logistic regression model and save output model in MORF /output directory
# usage: $ Rscript train_model_morf_mwe.R --course course_name_here
library(optparse)
# read command line argument for course
option_list = list(
    make_option(c("-c", "--course"), type="character", default=NULL, 
                help="course name", metavar="character"),
    make_option(c("-s", "--session"), type="character", default=NULL, 
                help="3-digit session ID", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
if (is.null(opt$course)){
    print_help(opt_parser)
    stop("Provide course name.", call.=FALSE)
}
if (is.null(opt$session)){
    print_help(opt_parser)
    stop("Provide course name.", call.=FALSE)
}

course = opt$course
session = opt$session
drop_cols = c("userID", "course", "session")
label_col = "label_value" # this will be binary indicator for dropout
dropout_week = 4
outpath = paste0("/output/", course, "_", session, "_mod.Rdata")

# iterate through session-level directories; read labels and feats; and merge into single file
input_dir = paste0("/input/", course)
runs =  list.files(input_dir)
session_data_list = list()
for (run in runs){
    run_dir = paste0(input_dir, "/", run)
    label_file = list.files(run_dir, pattern = ".*labels\\.csv$", full.names = TRUE, recursive = TRUE)
    feat_file = list.files(run_dir, pattern = ".*features\\.csv$", full.names = TRUE, recursive = TRUE)
    labels = read.csv(label_file)
    feats = read.csv(feat_file)
    session_data = merge(labels, feats)
    session_data_list[[run]] <- session_data
}
# create single dataframe from session_data_list; generate label_col; and drop unneeded columns
input_data = do.call("rbind", session_data_list)
# drop unwanted columns
mod_data = input_data[,!(names(input_data) %in% drop_cols)]
# convert binary outcome to factor
mod_data[,label_col] = factor(mod_data[,label_col])
# fit model
message("Fitting model for course ", course)
mod = glm(paste(label_col, " ~ .", sep = ""), data = mod_data, family = "binomial")
save(mod, file=outpath)

