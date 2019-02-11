import random
import logging
import os
from shutil import copyfile

from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext, SparkSession

fake_artist_ids = [i for i in range(1, 1000000)]
fake_track_ids = [i for i in range(1, 1000000)]
fake_listener_ids = [i for i in range(1, 101)]
fake_play_source_ids = [i for i in range(1, 30)]
artist_id_map = {}
track_id_map = {}
listener_id_map = {}

age_buckets = ['18-25', '26-40', '40-55', '65+']
genders = ['M', 'F', "Unknown"]
subscription_types = ['Ad-supported', 'Plus', 'Premium', 'Premium-family-plan']
play_sources = ['Station', 'Album', 'Collections', 'Playlist', 'Thumed Up Track', 'Thumbed Down Track', 'Autoplay', 'All Artist Tracks']

print "Making fake listener data"
with open('./data/fake_listeners.tsv', 'w') as listeners_out:
    listeners_out.write('\t'.join(['fake_listener_id', 'age', 'gender', 'subscription_type', 'country', 'fake_zipcode\n']))
    for fake_listener_id in fake_listener_ids:
        fake_age = age_buckets[random.randint(0, len(age_buckets)-1)]
        fake_gender = genders[random.randint(0, len(genders)-1)]
        fake_subscription_type = subscription_types[random.randint(0, len(subscription_types)-1)]
        fake_zipcode = str(random.randint(0, 99999))
        listener_id_map[id] = (fake_age, fake_gender, fake_subscription_type, 'US', fake_zipcode)
        listeners_out.write('\t'.join([str(fake_listener_id), fake_age, fake_gender, fake_subscription_type, 'US', fake_zipcode]))
        listeners_out.write('\n')

print "Done\nMaking fake spin data"
with open('/Users/bfemiano/Downloads/metadata.txt', 'r') as base_metadata:
    with open('./data/fake_spins.tsv', 'w') as out_data:
        lines = base_metadata.readlines()
        header = lines[0]
        out_data.write('\t'.join(["fake_artist_id", "artist_name", "fake_track_id", "track_title", "date_time", "elapsed_seconds", "play_source", "fake_listener_id\n"]))
        for line in lines[1:]:
            throttle = random.randint(0, 20) #Only keep 5% of the original data, just to keep size down. 
            if throttle == 1:
                (artist_id, artist_name, artist_uri, track_id, track_title, track_uri, isrc, upc, partner_id) = line.split('\t')
                index = random.randint(0, len(fake_listener_ids)-1)
                fake_listener_id = fake_listener_ids[index]
                fake_play_source = play_sources[random.randint(0, len(play_sources)-1)] 
                elapsed_seconds = random.randint(0, 300)
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                fake_datetime = "2019-02-08 %i:%i:%i" % (hour, minute, second)
                if artist_id in artist_id_map:
                    fake_artist_id, fake_artist_name = artist_id_map[artist_id]
                else:
                    index = random.randint(0, len(fake_artist_ids)-1)
                    fake_artist_id = fake_artist_ids[index]
                    del fake_artist_ids[index]
                    fake_artist_name = artist_name
                    artist_id_map[artist_id] = (fake_artist_id, artist_name)
                if track_id in track_id_map:
                    fake_track_id, fake_track_title = track_id_map[track_id]
                else:
                    index = random.randint(0, len(fake_track_ids)-1)
                    fake_track_id = fake_track_ids[index]
                    del fake_track_ids[index]
                    fake_track_title = track_title
                    track_id_map[track_id] = (fake_track_id, track_title)
                out_data.write('\t'.join([str(fake_artist_id), fake_artist_name,  
                                          str(fake_track_id),  fake_track_title,  
                                          str(fake_datetime), str(elapsed_seconds), fake_play_source, str(fake_listener_id)]))
                out_data.write('\n')
print "Done\nConverting to Parquet"

print "Writing parquet"
spark = SparkSession.builder.master('local').appName('blah').config(conf=SparkConf()).getOrCreate()

raw_listeners = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("inferSchema", "true").load("data/fake_listeners.tsv")
raw_listeners.write.parquet('./tmp_listeners_parquet')

raw_spins = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("inferSchema", "true").load("data/fake_spins.tsv")
raw_spins.write.parquet("./tmp_spins_parquet")

print "Copying Parquet data to basedir location"
listener_file = filter(lambda x: x.startswith('part'), os.listdir('./tmp_listeners_parquet'))[0]
copyfile("./tmp_listeners_parquet/%s" % listener_file, "data/listeners.snappy.parquet")
spins_file = filter(lambda x: x.startswith('part'), os.listdir('./tmp_spins_parquet'))[0]
copyfile("./tmp_spins_parquet/%s" % spins_file, "data/spins-2019-02-08.snappy.parquet")

print "Verifying parquet integrity"
print "--------------Sample 5 records--------------------"
listeners_df = spark.read.parquet('./data/listeners.snappy.parquet')
spins_df = spark.read.parquet('./data/spins-2019-02-08.snappy.parquet')
joined = spins_df.join(listeners_df, on='fake_listener_id')
for i in joined.take(5):
    print i
print "--------------------------------------------------"

print "Done"