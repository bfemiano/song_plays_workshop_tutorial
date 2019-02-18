# song_plays_workshop_tutorial

This is an advanced workshop for people already comfortable programming. Students will write 2 components. 

1.  A series of workflow management classes in Python using the Luigi framework. 
2.  A Scala Spark job to join the datasets together and perform some basic group analysis.

The workflow will chain together the processes of downloading multiple data sources for a given day (2019-02-08) from S3, sending those sources as input into the Spark program and
verifying expected output files are produced. 

It should take students with assistance about 2 - 3 hours including VM setup time to write from scratch. 

## Environment setup:

1. [VM Setup directions](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/VM_Setup.md) (Recommended)
2. For directions to setup the compile/run dependencies to run locally instead of the VM see [Local Setup](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Local_Setup.md).

## Workshop directions

1. First let's code the [Luigi Tasks](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Luigi_tasks.md).
2. Then let's write the [Scala Spark Job](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Scala_Spark.md).
3. Now let's run the Spark artifict you built with Luigi and [Put it all together](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Run_Directions.md).

## Fake data generation (Students can skip this)

For directions on how to run the fake data generator see [Fake Data Generator](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Data_Gen.md).