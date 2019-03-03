We're ready now to use the Scala spark application to process our downloaded data files. 

## Let's start!

### Step 1. Bring the jar you just built into a top-level ocation. 

In the VM run:

`cd /vagrant`.

Move the fat jar into the /vagrant working directory.

`mv scala_spark/build/libs/song_plays-1.0.0-jar-with-dependencies.jar  song_plays.jar`

CHECKPOINT: If you run `ls` in the working directory you should see at least the following items.

```
luigi_tasks  scala_spark  song_plays.jar
```

Also, running `echo $PYTHONPATH` should still show just `luigi_tasks`.

### Step 2. Launch the Spark luigi task again. 

Let's retry the Spark luigi task that failed from the early step. 

`luigi --module song_plays_tasks DatasetGen --date 2019-02-08 --local-scheduler`

You should see the Spark submit command print to the console and wait for a little while it runs.

```bash
spark-submit --master local --deploy-mode client --class com.song.plays.DatasetGen --driver-memory 1g --executor-memory 2g --driver-cores 1 --executor-cores 1 --num-executors 1 song_plays.jar --day 2019-02-08 --minrows 100 --listeners_path data/listeners/listeners.snappy.parquet --spins_path data/spins/2019/02/08/spins.snappy.parquet --dataset_out_path data/output/2019/02/08/dataset --analysis_out_path data/output/2019/02/08/counts_by_zip_sub
```

CHECKPOINT: By the time the process finishes you should see Luigi output `:)`. This means Spark successfully created the output directories and wrote _SUCCESS files to them, which meants 
all the partition output was successfully written. 

Now in the last step we'll inspect the output files to see what they look like. 


### Step 3. Take a look at the output you worked so hard to produce. 

1. From the same location you ran step 2, run the command `zcat data/output/2019/02/08/dataset/part*`

You should see output similar in structure to the below, although not necessarily in the same order. 

```
fake_listener_id	fake_artist_id	artist_name	fake_track_id	track_title	date_time	elapsed_seconds	play_source	age	gender	subscription_type	country	fake_zipcode
53	245237	2 Chainz	895745	Bff	2019-02-08T06:05:56.000-05:00	250	Autoplay	65+	Unknown	Plus	US	95101
82	245237	2 Chainz	690793	Starter Kit (feat. Young Dolph)	2019-02-08T05:46:51.000-05:00	166	All Artist Tracks	18-25	F	Family	US	25537
43	552933	Machel Montano	932803	Blazin' D Trail	2019-02-08T03:52:32.000-05:00	277	Playlist	18-25	M	Plus	US	87654
60	27915	Animosity	523051	Terror Storm	2019-02-08T00:51:37.000-05:00	228	Playlist	40-55	M	PremiumUS	92794
59	394231	Blackmill	201155	Spirit of Life	2019-02-08T19:40:22.000-05:00	295	Thumbed Down Track	40-55	M	Ad-supported	US	84255
......
```

2. Run `zcat data/output/2019/02/08/counts_by_zip_sub/part*`

You should see output similar to the below.

```
fake_zipcode	subscription_type	spins
10138	Premium	48
11489	Family	44
12138	Premium	41
14273	Ad-supported	40
14303	Ad-supported	43
....
```

Note: If you want to see the validation code you wrote in action, you can delete the contents under `data/output` and rerun the above command Luigi with `--minrows 10000000`. You'll see the Spark application throw an exception and Luigi show `:(`.

You have successfully completed this workshop!

Make sure you exit out of the VM SSH session and run `vagrant halt` to turn off the VirtualBox VM.  