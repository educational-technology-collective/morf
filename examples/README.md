# Examples

Toy example scripts mostly intended to test various MORF API functions for extraction, training, testing, and evaluation. 

For the best and most realistic modeling example (i.e., for use as a started script for your own modeling), see the scripts in `mwe/`.

For an introduction to MORF, see the [documentation](https://jpgard.github.io/morf/documentation/)

## morf-test-all

This example tests the MORF API functions `extract_all()`, `extract_holdout_all()`, `train_all()`, and `test_all()`. 

Note that running this example on a large data repository (including MORF's provided data) can be extremely slow and intensive; this is because the `_all()` function family loads the data from every session of every course. We strongly recommend not using the `_all()` function family unless your feature extraction requires this method (i.e., for normalization across all courses).

These are testing examples and are not intended to serve as good examples of predictive models! But, they can be used as basic starter code for more complex analyses.

| Dockerfile/Repo | Extract            | Extract-Holdout            | Train                    | Test                    | Evaluate                   | Complete?
| --------------- | --------------- | --------------- | --------------- | --------------- | --------------- | --------------- |
| `morf-test-all` | `extract_all()`            | `extract_holdout_all()`            | `train_all()`               | `test_all()`                 | `evaluate_course()` | Y
| `morf-test-course` | `extract_course()`            | `extract_holdout_course()`            | `train_course()`               | `test_course()`                 | `evaluate_course()` | Y
| `morf-test-session` | `extract_session()`            | `extract_holdout_session()`            | `train_session()`               | `test_course()`                 | `evaluate_course()` | Y
| `mwe` | `extract_session()` | `extract_holdout_session()`| `train_course()` | `test_course()`| `evaluate_course()`| Y