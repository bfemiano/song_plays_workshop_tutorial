import luigi
import os
from urllib2 import urlopen, HTTPError
from luigi.contrib.spark import SparkSubmitTask

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


def make_local_dirs_if_not_exists(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


class DownloadSpins(luigi.Task):

    date = luigi.DateParameter()
    url = "https://www.dropbox.com/s/92b6hqk2npyle6f/"
    file_name = "spins-{date:%Y-%m-%d}.snappy.parquet"

    def output(self):
        path = 'data/spins/{date:%Y/%m/%d}/spins.snappy.parquet'
        path = path.format(date=self.date)
        return luigi.LocalTarget(path)

    def requires(self):
        full_url = os.path.join(self.url, self.file_name.format(date=self.date))
        return ExternalFileChecker(url=full_url)

    def run(self):
        path = self.output().path
        make_local_dirs_if_not_exists(path)
        full_url = os.path.join(self.url, self.file_name.format(date=self.date))
        with open(path, 'w') as out_file:
            for data in urlopen(full_url).read():
                out_file.write(data)



class DownloadListeners(luigi.Task):

    date = luigi.DateParameter()
    url = "https://www.dropbox.com/s/5c7e4696qhqx53t/listeners.snappy.parquet"

    def output(self):
        data_path = 'data/listeners/listeners.snappy.parquet'
        path = 'data/markers/{date:%Y/%m/%d}/listeners_downloaded.SUCCESS'
        marker_path = path.format(date=self.date)
        return {'data': luigi.LocalTarget(data_path), 
                'marker': luigi.LocalTarget(marker_path)}


    def requires(self):
        return ExternalFileChecker(url=self.url)

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


    def requires(self):
        return {
            "listeners" : DownloadListeners(date=self.date),
            "spins": DownloadSpins(date=self.date)
        }

    def output(self):
        path = "data/output/{date:%Y/%m/%d}"
        data_success_path = os.path.join(path, 'dataset', '_SUCCESS').format(date=self.date)
        analysis_success_path = os.path.join(path, 'counts_by_zip_sub', '_SUCCESS').format(date=self.date)
        return {'dataset': luigi.LocalTarget(data_success_path),
                'analysis': luigi.LocalTarget(analysis_success_path)}

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
            "--dataset_out_path", os.path.dirname(data_success_path), # strip out _SUCCESS from path.
            "--analysis_out_path", os.path.dirname(analysis_success_path), # same. 
        ]
        return args
