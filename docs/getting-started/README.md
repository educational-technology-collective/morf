# Getting Started

You can install MORF using pip:

```
$ pip install morf
```

This installs all of the API functions needed to build end-to-end predictive modeling pipelines in MORF. Note that MORF is compatible with both Python 2 and 3, but for best results, we recommend using MORF with Python 3.6.

A complete example of a MORF job is available in the project [repository](https://jpgard.github.io/morf/); check out the `mwe` directory [here](https://github.com/jpgard/morf/tree/master/mwe)for a complete minimum working example. 

To execute a sample job, replace `your-email@example.com` below with your email address, and simply copy and paste the command into a terminal. 

```
$ curl 'https://dcd97aapz1.execute-api.us-east-1.amazonaws.com/dev/morf/?url=https://raw.githubusercontent.com/jpgard/morf/master/mwe/client.config&email_to=your-email@example.com'
```

If you receive a response similar to:

```
<p style="text-align: center;"><h2>Success! Your job has been submitted to MORF. You will receive an email from morf-alerts@umich.edu with status updates.</h2></p>23795
```

then the job has been submitted to MORF -- check your email for updates! MORF will notify you when the job is queued for execution, and when the execution initiates and completes.

If you need help creating your own jobs, use the [minimum working example](https://github.com/jpgard/morf/tree/master/mwe) as starter code, check the detailed [documentation](https://jpgard.github.io/morf/documentation/), or contact the MORF development team directly using the information listed on this site.
