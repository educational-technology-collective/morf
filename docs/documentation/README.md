# Documentation (under construction!)
{:.no_toc}

* Will be replaced with the ToC, excluding the "Contents" header
{:toc}

# MORF 2.0 API

This document describes available functions for predictive model training and testing in the MOOC Replication Framework (MORF) version 2.0. If you're new to MORF, this is the first document you should read.

# Introduction 

The MORF API is used to construct and evaluate predictive models from raw MOOC platform data. The main steps to executing a job on MORF are:

1. Write code to extract features from raw platform data and build predictive models on the results of that feature extraction.
2. Write a high-level "controller script" using the MORF Python API.
3. Build a Docker image containing your code and any software dependencies with the appropriate control flow.
4. Create a configuration file containing identifiers and links to your controller script and docker image.
5. Upload the the controller script, docker image, and configuration file to public locations (you need to provide either HTTPS or S3 URLs to these files).
6. Submit your job to the MORF web API. You will receive notifications and results via email as your job is queued, initiated, and completed.

To execute a complete example on MORF which extracts features from course clickstream data, trains a predictive model using logistic regression, and evaluates its dropout predictions, see the examples in  the [github repository](https://github.com/jpgard/morf) and the instruction on the [getting started](https://jpgard.github.io/morf/getting-started/) page.

# API Overview

MORF provides different API functions based on the way your modeling pipeline uses data. In particular, we provide multiple APIs for the `extract` and `train` steps of model-building. These functions are documented below, but each works in fundamentally the same way: the raw MORF data is mounted into a root-level `/input/` volume of the image along with the user-provided Docker image, and the Docker image is iteratively called using the `docker run` command with a `--mode` parameter specifying whether that image should `extract`, `train`, or `test`.

## Feature Extraction

Three types of API functions are available for feature extraction. These functions are used to specify whether the user-provided Docker image extracts features 

+ once per *session* (those ending in `_session()`), 
+ once per *course* (those ending in `_course()`),
+ once for all courses collectively (`_all()`)

Similar `extract_holdout` functions are used to specify how to extract features from holdout (test) data.

| Function name            | Description                    |
| ------------------------ | ------------------------------ |
| `extract_all()`       | Extracts features using the docker image across all courses and all sessions except holdout. If this function is used, the Docker image should iterate over the course- and session-level directory structure mounted in `/input/` and return a single *person* x *features* x *course* x *session* array. |
| `extract_course()` | Extracts features using the Docker image, building individual feature sets for each course. If this function is used, the Docker image should iterate over the session-level directory structure mounted in `/input/` and return a *person* x *features* x *session* array. The Docker image will be run once per course. This allows parallelization of feature extraction over courses. |
| `extract_session()` | Extracts features using the Docker image, building individual feature sets for each 'session' or iteration of the course. If this function is used, the Docker image should read data from the individual session directory structure mounted in `/input/` and return a *person* x *feature* array. The Docker image will be run once per session. This allows parallelization of feature extraction over sessions, and achieves the highest level of parallelization. We expect most users will utilize this function, unless normalization across courses or the entire dataset is necessary.|
| `extract_holdout_all()` | Extracts features using the Docker image across all courses and all sessions of holdout data.|
| `extract_holdout_session()` | Extracts features using the Docker image across each session of holdout data.|

To further clarify: the `extract_` function family determines how MORF runs your Docker image, and what data is mounted in the `/input/` directory each time that image is `run`. 

+ If `extract_all()` is used, the data for all runs of all courses is mounted to `/input/`, and the Docker image is `run` a single time. The Docker image is expected to write a single .csv file to `/output/` with a set of features for every user of every session of every course. The CSV should contain columns for 'course', 'session', and 'user'.
+ If `extract_course()` is used, data for an individual course is mounted to `/input/`, and the Docker image is `run` once for each course. The Docker image is expected to write a single .csv file to `/output/` with a set of features for every user for every session of the course. The CSV should contain columns for both 'session' and 'user'. MORF will aggregate and persist the features for each course internally (all your image needs to do is write them to the `/output` directory).
+ If `extract_session()` is used, data for an individual session of a course is mounted to `/input/`, and the Docker image is `run` once for each session of each course The Docker image is expected to write a single .csv file to `/output/` with a set of features for every user in that session. MORF will aggregate and persist the features for each session internally (all your image needs to do is write them to the `/output` directory).

Note that there is no `extract_holdout_course()` function because there is only one session per course; this would be equivalent to using `extract_holdout_session()`.

All feature extraction functions expect the Docker image to write individual `.csv` files to `/output` at the level of aggregation of the `extract` function used: `extract_all()` should write a a single *user x feature* .csv file to `/output/`; `extract_course()` should write one *user x feature* .csv array for each course; `extract_session()` should write one *user x feature* .csv array for each session of each course.

The complete specifications for the `/input/` directory structure are in shown in the input-output documentation below. The full set of `docker run` parameters used to call the image are demonstrated in the docker section below.

If your workflow does not conform to the expected input/output protocol, no data will be returned and the job will be cancelled.

## Model Training

Three types of API functions are also available for model training. Identical to the feature extraction options, these functions are used to specify whether the user-provided Docker image trains one model per *session* (`train_model(session)`), once per *course* (`train_model(course)`), or once overall for the entire MORF 2.0 dataset (`train_model()`). These can be used regardless of how feature extraction was conducted.

| Function name            | Description                    |
| ------------------------ | ------------------------------ |
| `train_all()` | Trains a single overall model using the entire dataset using the Docker image. Features for each session of the course will be aggregated (if not aggregated already) and provided in a single *person* x *feature* array to train the model.|
| `train_course()` | Trains one model per course using the Docker image. Features for each session of the course will be aggregated (if not aggregated already) and provided in a single person x feature x session] array to train the model.|
| `train_session()` | Trains one model per session of the course using the Docker image.|


Your code is expected to output one file at the level of aggregation of the `train` function used (if you use `train_course()`, your code should write one file per course in `\output`). This file should contain any elements of your trained model that will be needed for prediction on new data in the `test` step. The type of file is arbitrary (it can be any file type) and should match the type of file your Docker image expects as input in `test` mode.

## Model Testing

To test your models, MORF applies the trained models to the features extracted from the holdout session of each course, using the features extracted in the `extract-holdout` step. 

Note that while there are several options for feature extraction and predictive model construction, there is only one method available for predictive model evaluation. This is because no matter how models are constructed, they are evaluated on held-out runs of the same courses used for training. No matter whether models are trained at the session, course, or overall level, they will be used to predict on features from the held-out datasets extracted using the method specified above, and these performance results are aggregated and returned to you. For more information on why MORF uses this prediction architecture, see the MORF software paper in [publications](https://jpgard.github.io/morf/publications/).

| Function name            | Description                    |
| ------------------------ | ------------------------------ |
| `test_all()`  | Evaluates a single model on all courses. This should be the testing function used with `extract_holdout_all()`.|
| `test_course()`  | Evaluates a model each course individually. This should be the testing function used with `extract_holdout_session()`.|

Your code is expected to write individual `.csv` files to `/output` at the level of aggregation of the `extract_holdout` function used. This file should have three columns: `userid` (the first column), `pred` (the predicted label), and `prob` (the probability of this label). The first row of the file should contain the column names. 

# API Detail: Feature Extraction, Directory Structure, and Input/Output Contract

MORF provides flexibility over how features are extracted for predictive model evaluation in the platform. In order to do so, but ensure that the extract-train-test pipeline functions seamlessly, we utilize a specific *input/output contract* for each family of functions: 

+ the *input* directory structure is consistent, always located at `/input`. Raw data for every course is organized within directories `/input/course/session/` where `course` is a course name and `session` is a unique session id. Docker images should always expect this structure whenever input is used for the API functions (i.e., for `extract`, `train`, and `test` families).
+ the *output* structure is consistent. For feature extraction and model testing, output is generally expected to be a .csv file, one per iteration of extraction or testing. For model training, output can be any type of file, and should match the file type expected as input to your Docker image when called in `test` mode.

For more detailed information on MORF's input-output contract, see `documentation/input-output/README.md`

# Docker Image environment

The Docker image you provide will be run in a non-networked environment with `/input/` and `/output/` directories mounted in its filesystem. Because the Docker image does not have network access (for security reasons), any required libraries or software packages should be already installed in the Docker image.

Your Docker image will be called with a `--mode` flag, which is used to trigger the appropriate action based on the input that will be mounted in its filesystem. These `mode`s are:

+ `--mode=extract`: Raw course data will be mounted in `/input/` directory; feature extraction expected according to input/output contract described above.
+ `--mode=train`: Extracted data from training runs will be mounted in `/input/` directory; model training expected according to input/output contract.
+ `--mode=test`: Extracted data from testing runs will be mounted in `/input/` directory; model testing/prediction expected according to input/output contract.

For more information on setting up a Docker image that works with MORF, see `documentation/docker/README.md`.


