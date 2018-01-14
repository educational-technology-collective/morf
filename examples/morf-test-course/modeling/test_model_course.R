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

## script to test pre-trained logistic regression model and save dataframe of [course,username,predictions] to MORF /output directory
# usage: $ Rscript test_model_course.R --course course_name_here
library(optparse)
# read command line argument for course
option_list = list(
    make_option(c("-c", "--course"), type="character", default=NULL, 
                help="course name", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
if (is.null(opt$course)){
    print_help(opt_parser)
    stop("Provide course name.", call.=FALSE)
}

course = opt$course
id_col = "userID"
drop_cols = c("course", "session")
outpath = paste0("/output/", course, "_test_preds.csv")
thresh = 0.5 # threshold for predicting dropout

# find csv files of features and model; read, merge, and drop relevant columns in test data
input_dir = "/input"
feat_file = list.files(input_dir, pattern = ".*features\\.csv$", full.names = TRUE, recursive = TRUE)
mod_file = list.files(input_dir, pattern = "Rdata$", full.names = TRUE, recursive = TRUE)
# check data and model files; stop execution if not detected
if (length(mod_file) == 0){
    stop("No model file detected for course ", course)
}
if (length(feat_file) == 0){
    stop("No feature file detected for course ", course)
}
test_data = read.csv(feat_file)
rownames(test_data) <- test_data[,id_col]
# drop unwanted columns
test_data = test_data[,!(names(test_data) %in% drop_cols)]
# read model and predict on test data
load(file=mod_file) # loads mod_file; note that model will be loaded as whatever variable name it was saved as during training
# predict using model
pred_probs = predict(mod, newdata = test_data, type = "response")
pred_labs = ifelse(pred_probs > thresh, 1, 0)
# write output
pred_df = data.frame("userID" = names(pred_probs), "prob" = pred_probs, "pred" = pred_labs)
write.csv(pred_df, file = outpath, row.names = FALSE)

