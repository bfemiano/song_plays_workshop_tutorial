
## Overview

In this section we will write the Python tasks using the Luigi framework to manage our job workflow.

We'll start with the Download task for spins, then the Download task for listeners, followed by 
the the Spark dataset generation and analysis job.  

Note that we will check our progress multiple times along each step to make sure we're on track for success. 
When you see `CHECKPOINT` there will be one or more commands you can run to check your progress. If you have the expected output you can continue
to the next section. If not you might have made a mistake writing the code or setting up an environment variable. 


## What is Luigi?

https://github.com/spotify/luigi

Luigi is a very lightweight dependency management framework for batch workflows. In our case we can use it to make sure
the dataset generation and analysis job doesn't run until the download jobs have both succeeded. It will also be useful
for launching the Spark command with different parameterized input, and also verifying that the job produces some output
for a given day. 

In terms of what you'll see used in this workshop from Luigi:

1. Target = An abstraction around some external artifact. Either a file on disk, S3, a directory, etc. 
    * Targets define exists(), which returns True/False. (I.E. A file on disk would return True if the path specified exists. Otherwise False.)
2. Task = Defines a unit of work to be executed with the run() method.
    * Tasks are considered complete if every target returned by output() exists. 
    * Tasks are ready to run if every task in their requires() is complete. 
3. Parameters = Let us customize an instance of a task. The canonical example of this being for different dates. 


## Let's start!

### Step 1. Python module to hold luigi tasks.

On your local machine in the same directory as your Vagrantfile, create a subdirectory for `luigi_tasks`.
Inside this subdirectory let's create `song_plays_tasks.py`

CHECKPOINT: If you're logged into the VM, run: `ls /vagrant/luigi_tasks/` should show a single `song_plays_tasks.py` file.


### Step 2. Create DownloadSpins task

First we need a task to download the daily file containing events for every single instance a song played on Pandora. 

On your local machine (since it'll be easier for development most likely), open the file you just created `song_plays_tasks.py` in your preferred Python editor. 

First let's start the code to download the spins parquet data file on S3. 

Let's take advantage of Luigi parameters and give this class a Date parameter. This will
let us create instances with different dates. 

```python
import luigi
import os
from urllib2 import urlopen, HTTPError

class DownloadSpins(luigi.Task):

    date = luigi.DateParameter()
    url = "https://www.dropbox.com/s/92b6hqk2npyle6f/"
    file_name = "spins-{date:%Y-%m-%d}.snappy.parquet?dl=1"
```
The url is a constant fixed to the static location on dropbox where these files reside. 
the file_name value you'll notice has a formatted `date` area. This will be used later. 

Just below this line in the same class let's implement a few methods. 
Note: Make sure these are scoped inside the class you defined above. This will require indenting them. 

```python
def output(self):
    path = 'data/spins/{date:%Y/%m/%d}/spins.snappy.parquet'
    path = path.format(date=self.date)
    return luigi.LocalTarget(path)

def get_full_url(self):
    full_url = os.path.join(self.url, self.file_name.format(date=self.date))
    full_url = full_url if "2019-02-08" in full_url else full_url.replace('92b6hqk2npyle6f', '1234')
    return full_url 

def run(self):
    path = self.output().path
    make_local_dirs_if_not_exists(path)
    with open(path, 'wb') as out_file:
        for data in urlopen(self.get_full_url()).read():
            out_file.write(data)
```

The output() method defines a single local target for where we expect the file to be after the download finishes. 
The run() method implements the code to download the file from the url into the expected output location. 

You should now have a full class for `DownloadSpins` that doesn't require any tasks to run.

Before we can run this task though you might have noticed we're missing the method `make_local_dirs_if_not_exists`.
This will end up being useful across all of our tasks, so we'll create it at the module level. 

Just above the class definition line for `class DownloadSpins`  add the module-level function. 

```python
def make_local_dirs_if_not_exists(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
```

CHECKPOINT: Let's try running this class for a given date. 

1. Inside the VM, cd to the VM directory `/vagrant`, 
2. `export PYTHONPATH=luigi_tasks`
3. `luigi --module song_plays_tasks DownloadSpins --date 2019-02-08 --local-scheduler`

We send `--local-scheduler` since for this workshop we haven't setup a centralized scheduler and want to run Luigi in just a single local interpreter. 

You should see Luigi output `:)` to indicate the task was successfully run. 

We're missing something though...

Try running it for a different day that doesn't have data in the remote Dropbox location. Let's try 2019-02-09

`luigi --module song_plays_tasks DownloadSpins --date 2019-02-09 --local-scheduler`

We get an error. 

Before we're ready to move on, we need to add a dependency on a task that checks if the file is actually available in the remote location.

Let's add the following method to our `DownloadSpins` task.

```python
def requires(self):
    return ExternalFileChecker(url=self.get_full_url())

```

This tells our `DownloadSpins` task not to run if the `ExternalFileChecker` instance for the URL isn't complete.
The url is formatted to take into account the date argument passed in from the caller. In this case the caller is our
instance of `DownloadSpins`, which has it's own `date` parameter it can just pass down. 

In order for this to work though we need to actually write the class `ExternalFileChecker` and it's accompanying target.

At the top of the class just under our imports, add the following code:

```python
class HttpTarget(luigi.Target):

    def __init__(self, url):
        self.url = url

    def exists(self):
        try:
            urlopen(self.url)
            return True
        except HTTPError:
            return False


class ExternalFileChecker(luigi.ExternalTask):

    url = luigi.Parameter()

    def output(self):
        return HttpTarget(self.url)
```

This is our custom HttpTarget that defines exist() as the url being accessible.
If urlopen() throws any HttpError, we consider the target unavailable at the URL. 
We can use this to build a special kind of Luigi task called an `ExternalTask`, which
is just a task that checks for completeness using output(), but isn't expected to run 
anything if the task isn't initially satisfied. This comes in extremely handy in the real
world when we want to model dependencies outside our control, but not have the pipeline
throw errors if certain dependencies are not ready. In our example, we don't control
the data availability at the remote location on Dropbox, so we model that as an external dependency. 

CHECKPOINT: Now let's try the `DownloadSpins` task again for 2019-02-09.

`luigi --module song_plays_tasks DownloadSpins --date 2019-02-09 --local-scheduler`

You should see the Luigi `:|` output. This means the scheduler was unable to run
one or more tasks because of an external dependency not being satisfied. In our case,
there is no spins datafile for 2019-02-09. For the rest of the workshop we'll stick
with the date we know has data: 2019-02-08.

Additional notes: If you browse under `/vagrant/data/` you should see the file in the directory tree 
for `data/spins/2019/02/08/spins.snappy.parquet`. This is an effective
data Hadoop HDFS and S3 partitioning indexing strategy for laying out large collections of data we receive as somewhat predictable batch updates, usually on a daily or hourly basis. 
You can very easily send globbed file-paths to programs for let's say an entire month using for example `--input data/spins/2019/02/*`. In that example, we would send all the spins data for Feb 2019 to some program. 

The file was written as snappy-compressed Parquet. I used snappy because it achieves a relatively good compression ratio without much CPU intensity. I used Parquet because it provides a very efficient means of storing columnar data for various queries involving sparse projections over the data. 

### Step 3. Create DownloadListeners task

Underneath the spins task let's start the listeners task. 

```python
class DownloadListeners(luigi.Task):

    date = luigi.DateParameter()
    url = "https://www.dropbox.com/s/5c7e4696qhqx53t/listeners.snappy.parquet?dl=1"

    def requires(self):
        return ExternalFileChecker(url=self.url)
```

The listeners dimension keeps track of all Pandora listeners that could potentially generate song spin events. We're interested in reporting to the music labels which listener IDs were responsible for those song spins. 

You'll notice right away that although this task also takes in a date parameter, it doesn't use it for
format the url we check against. We will instead use the date parameter to keep a marker file at a subdirectory location that matches the date. If the marker exists, that means we already successfully downloaded the listeners file on the given date. 

This was setup is designed to illustrate a slightly different data access pattern that comes up
a lot in the real world. That being a static lookup file that doesn't change in name as it's updated. Often you see this with smaller append-only datasets that get updated periodically and reposted to shared locations. Teams go through quite a bit of trouble to manage this pattern, often keeping track of file modification times or comparing byte counts to see if something has changed. For our
case though we can use a simple pattern. Let's log a marker file for every day we've successfully downloaded the file. This will keep our task from redownloading the listeners file
on a day that it has already retrieved the latest file. This pattern works well for dimensions where we can use the newest version of the file to reprocess older days. 

Let's add the code to create the marker file at the right subdirectory for the date parameter:

```python
def output(self):
    data_path = 'data/listeners/listeners.snappy.parquet'
    path = 'data/markers/{date:%Y/%m/%d}/listeners_downloaded.SUCCESS'
    marker_path = path.format(date=self.date)
    return {'data': luigi.LocalTarget(data_path), 
            'marker': luigi.LocalTarget(marker_path)}
```

In this case we want to make sure the task both produced a valid file and that a marker file was successfully written for the date argument. This is why
we have 2 output targets from this task. 

Notice you'll see how in this output() we return a dictionary, whereas the download spin task returned just a single object. Luigi can handle most
different types of return types that come from output(), including lists, tuples, dictionaries, objects and even generators. 

Using dictionaries let's use label the target references for ease of use in our run method, which we'll add next:

```python
def run(self):
    data_path = self.output()['data'].path
    marker_path = self.output()['marker'].path
    make_local_dirs_if_not_exists(data_path)
    make_local_dirs_if_not_exists(marker_path)
    with open(data_path, 'w') as out_file:
        for data in urlopen(self.url).read():
            out_file.write(data)
    with open(marker_path, 'w') as out_file:
        pass
```

Note: It's very common in cases where tasks have two or more output targets to use the run method to and remove any targets that might already exist. I purposefully omitted this design pattern from the workshop, because I don't want students to have to write removal code that could go wrong and remove too much.

You might be thinking "how can the task be running if a target already exists and would need to be cleaned up?". Remember, Luigi will run the task if even one output target it expects isn't available, but often times in that scenario running the task when other output targets are already complete can cause side-effects. It's best to just start with a clean slate of output targets before each run. 

All this run method does is download the data at the supplied url parameter (which is constant) and then create the daily marker file in the right subdirectory location under `/vagrant/data`

CHECKPOINT: Let's make sure this works for 2019-02-08

`luigi --module song_plays_tasks DownloadListeners --date 2019-02-08 --local-scheduler`

You should see the luigi `:)` signal that the file was downloaded correctly and that the marker file was created. 
If you rerun the same command, it shouldn't redo the file, but should also give the `:)` signal that the work was already complete and there was nothing more to do. 


### Step 4. Spark task. 

Now that we have our two download tasks implemented and working, let's implement the Luigi task that will actually launch our Spark job. 

At the top of the module in the imports section, add the below import.

`from luigi.contrib.spark import SparkSubmitTask`

This is a handy Luigi task that handles formatting the `spark-submit` command with the right config options. 

Now let's start the class implementation that will extend `SparkSubmitTask` to run our specific Spark task. 

We can use Luigi parameters to configure the different spark settings we want, such as the number of executors, memory per executor,
executor cores, driver memory, etc.

```python

class DatasetGen(SparkSubmitTask):
    date = luigi.DateParameter()
    minrows = luigi.IntParameter(default=100)

    # Spark properties
    driver_memory = '1g'
    executor_cores = 1
    driver_cores = 1
    executor_memory = '2g'
    num_executors = 1
    deploy_mode = 'client'
    spark_submit = 'spark-submit'
    master = 'local'
    app = 'song_plays.jar'
    entry_class = 'com.song.plays.DatasetGen'
```

date and minrows are custom parameters defined for just our subclass. Date will be passed to the Spark program. 

Minrows will play an important role for validation and will also need to be passed to the Spark job. 

Note that the jar and class don't exist yet, but will once we finish the Scala spark work in the next step. 

Next we have to define the dependencies this job has. We want to make sure before we launch the Spark job that the luigi
tasks to download the spins and listeners for a given day are both complete.

```python
def requires(self):
    return {
        "listeners" : DownloadListeners(date=self.date),
        "spins": DownloadSpins(date=self.date)
    }
```

We also want to define any output targets we expect the Spark job to produce. For now we expect it to 
produce two output directories, one for the dataset generation and another for the analysis. We also
want to explicitly look for the presence of _SUCCESS files in those locations, since that is a signal
Spark uses that it wrote all partition data to the location without fault. 

```python
def output(self):
    path = "data/output/{date:%Y/%m/%d}"
    data_success_path = os.path.join(path, 'dataset', '_SUCCESS').format(date=self.date)
    analysis_success_path = os.path.join(path, 'counts_by_zip_sub', '_SUCCESS').format(date=self.date)
    return {'dataset': luigi.LocalTarget(data_success_path),
            'analysis': luigi.LocalTarget(analysis_success_path)}
```

We don't need to implement the run() method in this case, because the parent class takes care of that
for us. We do however need to override one important method that defines any app parameters we 
want to pass to our Spark driver. 

```python
def app_options(self):
    reqs_dict = self.requires()
    outs_dict = self.output()
    listeners_path = reqs_dict['listeners'].output()['data'].path
    spins_path = reqs_dict['spins'].output().path
    data_success_path = outs_dict['dataset'].path
    analysis_success_path = outs_dict['analysis'].path
    args = [
        "--day", "{date:%Y-%m-%d}".format(date=self.date),
        "--minrows", self.minrows,
        "--listeners_path", listeners_path,
        "--spins_path", spins_path,
        "--dataset_out_path", os.path.dirname(data_success_path), # strip out _SUCCESS from tail of path.
        "--analysis_out_path", os.path.dirname(analysis_success_path), # same. 
    ]
    return args
```

In our case we want to pass 6 different arguments to the Spark driver.
1. The day argument. (I.E 2019-02-08)
2. The minimum number of expected rows to validate against.
3. An input path to the listeners file we downloaded.
4. An input path to the daily spins file we downloaded. 
5. An output directory to write the dataset produced by Spark. 
6. An output directory to write the analysis produced by Spark. 

CHECKPOINT: Try running the below command: 

`luigi --module song_plays_tasks DatasetGen --date 2019-02-08 --local-scheduler`

You should get the error for `java.lang.ClassNotFoundException: com.song.plays.DatasetGen`.


More specifically, you should see that the two download tasks were satisfied, and that the job failed to run the Spark job. 
```
Scheduled 3 tasks of which:
* 2 complete ones were encountered:
    - 1 DownloadListeners(date=2019-02-08)
    - 1 DownloadSpins(date=2019-02-08)
* 1 failed:
    - 1 DatasetGen(date=2019-02-08, minrows=100)

This progress looks :( because there were failed tasks
```

Now we're ready to actually make our Spark jar. 

See the next section for [Scala Spark direction](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Scala_Spark.md)