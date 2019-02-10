
Wire and explains Spins Download task first. 
Then show requires() external task with notes below.  
Then explain Listeners download task. Explain why it has an additional target. 

luigi --module song_plays_tasks DownloadSpins --date 2019-02-08 --local-scheduler
luigi --module song_plays_tasks DownloadListeners --date 2019-02-08 --local-scheduler

make the file song_plays_tasks.py 


export PYTHONPATH=`pwd` 
luigi --module song_plays_tasks ExternalFileChecker --url https://s3.amazonaws.com/storage-handler-docs/listeners.snappy.parquet --local-scheduler

Checkpoint:
    Running with --url = https://s3.amazonaws.com/storage-handler-docs/listeners.snappy.parquet should show :)
    Running with --url = some non-existance URL should show :| 