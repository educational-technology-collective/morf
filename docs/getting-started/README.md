# Getting Started

MORF is available on [PyPi](https://pypi.python.org/pypi/morf-api). You can install MORF using pip:

```
$ pip install morf-api
```

This installs all of the API functions needed to build end-to-end predictive modeling pipelines in MORF. Note that MORF is compatible with both Python 2 and 3, but for best results, we recommend using MORF with Python 3.6.

A complete example of a MORF job is available in the project [repository](https://github.com/educational-technology-collective/morf); check out the `mwe` directory [here](https://github.com/educational-technology-collective/morf)for a complete minimum working example. 

To execute a sample job, replace `your-email@example.com` below with your email address, and execute this code in a Python interpreter. 

```
>>> from morf.utils.submit import submit_mwe
>>> submit_mwe(email_to="your-email@example.com")
```

If you want to use a different MORF example or submit your own job, use `easy_submit()` in a Python interpreter:

```
>>> from morf.utils.submit import easy_submit
>>> easy_submit(client_config_url="https://raw.githubusercontent.com/educational-technology-collective/morf/master/examples/morf-test-session/client.config", email_to="your-email@example.com")
```

You can also make submissions using `curl`:
```
$ curl 'https://dcd97aapz1.execute-api.us-east-1.amazonaws.com/dev/morf/?url=https://github.com/educational-technology-collective/morf/blob/master/mwe/client.config&email_to=your-email@example.com'
```

For any of the above methods, if you receive a printed response similar to:

```
<p style="text-align: center;"><h2>Success! Your job has been submitted to MORF. You will receive an email from morf-alerts@umich.edu with status updates.</h2></p>23795
```

then the job has been submitted to MORF -- check your email for updates! MORF will notify you when the job is queued for execution, when the execution initiates and completes various stages of the workflow, and (if you have logging access) will provide notifications when an exception occurs.

If you need help creating your own jobs, we suggest using the [minimum working example](https://github.com/educational-technology-collective/morf/tree/master/mwe) as starter code, check the detailed [documentation](https://educational-technology-collective.github.io/morf/documentation/), or contact the MORF development team directly using the information listed on this site.
