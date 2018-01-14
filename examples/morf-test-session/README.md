# MORF Minimum Working Example (MWE)

This diretory holds a complete, self-contained example of scripts which `extract` structured features from raw data in MORF including the training and `holdout` sessions, `train` a simple logistic regression model using those features, and `test` that model.

## Running the MWE

Running the MWE is a simple as executing the master script, `run_morf_mwe.py`:

``` 
$ python3 run_morf_mwe.py
```
 
This script, reproduced below, contains the MORF Python API functions needed to parallelize execution of the MORF workflow, including loading the Docker image in MORF's secure computing environment, extracting features for training and holdout runs, training the model, testing the model, and evaluating the results.

``` 
"""
A test script to extract features, train a model, and evaluate it using the MORF 2.0 architecture.
"""

from morf.morf_api.extract import extract_session, extract_holdout_session
from morf.morf_api.train import train_course
from morf.morf_api.test import test_course
from morf.morf_api.evaluate import evaluate_course


extract_session()
extract_holdout_session()
train_course(label_type = 'dropout')
test_course()
evaluate_course(label_type = 'dropout')
```

We designed the MORF Python API to be as simple as possible, while allowing flexibility with how users want their data loaded (at the session level, the course level, or over the entire MORF dataset at once).

## Components of the MWE

#### Master MORF script

The master script, `run_morf_mwe.py`, is described above. This controls the execution of the Docker image, and handles things like parallelization, loading and mounting data within the Docker container, and persisting the results of each step. You cannot run a job on MORF without a master script.

#### Controller script

The controller script, `mwe.py`, is the `entrypoint` for the Docker image: whenever the image is `run`, it is provided with command-line variables that provide information to direct the subsequent workflow of the script. The most important of these is the `mode` parameter. Every controller script should (a) parse the execution arguments to obtain the `mode` parameter, and (b) provide conditional logic to execute the appropriate behavior for each of the 4 possible values of `mode`. These are:

* `extract`: controls feature extraction for training sessions from raw course data.
* `extract-holdout` controls feature extraction for testing (holdout) sessions from raw course data.
* `train`: trains a model, using output from `extract` step as the input data.
* `test`: tests a model, using output from `extract-holdout` as the testing data and the model from the `train` step as the predictor.

For more information on the use of the `mode` parameter, see the `documentation` directory.

#### Modeling Scripts

The `/mwe/modeling` directory contains two scripts, written in R, that train and test models, respectively. 

The training script, `train_model_morf_mwe.R`, iterates through session-level data directories, reading the `feats.csv` and `labels.csv` files, merging on userID, and concatenating into a single dataframe for model training. It saves the model, as a `.Rdata` file, into the `/output/` directory mounted to the Docker image. Note that data for each session of a course is mounted because the master script uses `train_course()`, which mounts course-level data (including all training sessions, if multiple sessions exist). The training script can save a model file of any type; make sure your testing script expects that file type.

The testing script, `test_model_morf_mwe.R`, reads the `feats.csv` file (no labels are provided for testing), loads the model, and saves a csv of predictions into the `/output/` directory mounted to the Docker image. Note that the csv contains columns for `userID`, `prob` (the predicted probability of dropout), and `pred`, the predicted binary dropout label (0 or 1).

Below is the head of the csv file generated as output from the MWE model testing script:

``` 
userID,prob,pred
577565faad64aee5941e5cfe0bfb771f494ef38c,0.854974830451673,1
3a123c4b417a4d2581705bd1745b733e49044a88,0.854974830451673,1
af74b2124ed39c35ae4bd172d77f8925c05bee7d,0.854974830451673,1
92d4d4d8f7d8d11b7c8dc6035aa9daf027cc7b13,0.854974830451673,1
7c14ee25f4484a92656201a336f45e8c61ff30d1,0.854974830451673,1
083f2a394144d0ce95d0c4fb4fbbd12c16f2bf86,0.854974830451673,1
6ce838c981b18de252e987c263bb78292e628fd0,0.854974830451673,1
0ec50cd0bf925830789241daa7529a5d00f481de,0.854974830451673,1
```

`prob` is the probability of being in the positive class (in this case, dropout probability), and `pred` is the predicted class label. This isn't a very good model, because it only looks at the number of forum posts in the first 4 weeks of the course.

#### Feature Extraction

Several data sources are available for feature extraction; for a full listing of the Coursera data export formats, see [the Coursera data exports documentation](https://spark-public.s3.amazonaws.com/mooc/data_exports.pdf).

The feature extraction module essentially completes two tasks. First, it loads the relevant SQL dumps into the mySQL server and executes queries to obtain counts of forum posts and forum comments by user, using `extract_coursera_sql_data()`. The source code for `extract_coursera_sql_data()` is in `mwe/feature_extraction/sql_utils.py`. Second, this script executes the main function of `mwe/feature_extraction/mwe_feature_extractor.py`, which writes a csv of weekly counts of forum posts, by user, to the `/output/` directory in the Docker image for each individual session. 

Below is the head of the csv file generated as output from the MWE feature extraction script:

``` 
userID,week_0_forum_posts,week_1_forum_posts,week_2_forum_posts,week_3_forum_posts,week_4_forum_posts
4129374b319d833e02d3f347252173ef1860fd78,0,0,0,0,0
ad37d6a9143689f5167b530e0fef3626f1d07ecc,0,0,0,0,0
6298864c698b888ee60c47d52b70c0ce13adbae5,0,0,0,0,0
0b19ded193a829ca868d50fe724932096e6e84e6,0,0,1,0,0
28779ae0722051d556d8f867f55efcefed98c4b2,0,0,0,0,0
e66670716c0ef4c5b9853a1f95d518517843d62f,0,0,0,0,0
318a3f98eec18f5991d37a7521228da6f278e541,0,0,0,0,0
06e66baca75566aa7d946d55ddb0e75812768ef4,0,0,0,0,0

```

Note that the feature extraction process for the MWE assumes that only data from a single session is mounted in the Docker image; this is because the master script uses `extract_session()`.

