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
# usage: $ Rscript test_model_all.R 

id_col = "userID"
drop_cols = c("course", "session", id_col)
outpath = "/output/test_preds.csv"
thresh = 0.5 # threshold for predicting dropout
input_dir = "/input"

# load model after check data and model files; stop execution if not detected
mod_file = list.files(input_dir, pattern = "Rdata$", full.names = TRUE, recursive = TRUE)
if (length(mod_file) == 0){
    stop("No model file detected for course ", course)
}
load(file=mod_file) # loads mod_file; note that model will be loaded as whatever variable name it was saved as during training

# iterate through session-level directories; read feats; predict; and save into dataframe
session_data_list = list()
for (course in list.files(input_dir)){
    for (session in list.files(paste(input_dir, course, sep="/"))) {
        dirname = paste(input_dir, course, session, sep = "/")
        feat_file = list.files(dirname, pattern = ".*features\\.csv$", full.names = TRUE, recursive = TRUE)
        test_data = read.csv(feat_file)
        rownames(test_data) <- test_data[,id_col]
        # drop unwanted columns
        test_data = test_data[,!(names(test_data) %in% drop_cols)]
        # predict using model
        message(paste0("[INFO] generating test predictions for course ", course))
        pred_probs = predict(mod, newdata = test_data, type = "response")
        pred_labs = ifelse(pred_probs > thresh, 1, 0)
        df_out = cbind(userID = data.frame(userID = row.names(test_data)), prob = pred_probs, pred = pred_labs, course = course)
        row.names(df_out) = NULL
        session_data_list[[paste0(course,session)]] <- df_out
    }
}
# create single dataframe from session_data_list; generate label_col; and drop unneeded columns
pred_df = do.call("rbind", session_data_list)
write.csv(pred_df, file = outpath, row.names = FALSE)

