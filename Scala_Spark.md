This is where we'll write the Scala code to power our Spark application using the arguments passed from the command line by Luigi.

It's recommended you have finished the last checkpoint from Luigi Tasks before starting on these directions. 

The goal of this spark application is to combine the listeners dimension with the daily spins data and produce 2 output artifacts.

1. A dataset where each spins has the relevant listener information joined by listener id. 
2. An aggregate report of spins counted by zipcode and subscription type groups. For example "The number of times Ariana Grande 'Thank you, Next' played in zip code 90120 for ad-supported users was X."

## What's this for again?

The labels are often very interested in receiving these types of geo reports, and also receiving the raw spins dataset collections with some additional attributes spliced in.

Some additional attributes joined in production, but not in this workshop:
1. Listener information. We're working with just this in our workshop for simplicity sake. 
2. Album properties.
3. Composer information.
4. Track releases information.
5. Details about the Pandora radio station it played on (if applicable)
6. If a genre station, additional details.
7. If a played from a playlist, additional details about the playlist. 


We want to build a scala application that can read:

1. The day for which we're generating the report.  
2. The minumum number of rows we expect in the dataset output file. Throw an error if this is not reached. 
3. A local file-system input path to the listeners file.
4. A local file-system input path to the daily spins file for the given day we're actively processing. 
5. A local file-system output path for the dataset generation.
6. A local file-system output path for the aggregate analysis.  

## Let's start!

### Step 1. Create the build workspace. 

Assuming working dir under `/vagrant` let's start to create the scala workspace.

Let's also assume we're going to name our scala basedir location `scala_spark`

Inside the VM run: `mkdir -p /vagrant/scala_spark/src/{main,test}/{scala,resources}`

This will setup our project build and test structure. 

### Step 2.  Setup Gradle

Let's use Gradle to build a jar to submit to Spark very easily. 

SBT (Scala build tool) is a very popular scala build system, but for this small project it's unnecessarily complicated. Moreoever, many data engineering teams
have Java for other applications and often find themselves using Gradle for those environments. It's nice to be have a consistent build system across Java and Scala applications. 

There are advantages to SBT over Gradle for Scala builds, but for this project we can keep it really simple. 

Let's copy the gradle wrapper launcher from the student home directory to the scala build workspace.  The gradle wrapper is a handy utility that let's you bundle a specific version
of gradle on a per-application basis rather than have to rely on the system install (if any). We find this a very useful setup on our teams, especially in our CI environments. 

In the VM: 

```bash
cd /vagrant/scala_spark
cp -r ~/gradle .
cp ~/gradlew  .
```

### Step 3. Create build.gradle and settings.gradle files.

Let's make a build that can bundle our utility and all dependencies, including the scala-lib runtimes, into a fat jar.

Create a file under `/vagrant/scala_spark` called `build.gradle`. 

```groovy
description = 'Spark job for dataset generation and analysis'
version = "1.0.0"


apply plugin: 'java'
apply plugin: 'scala'
apply plugin: 'com.github.johnrengelman.shadow'

sourceCompatibility = 1.8
targetCompatibility = 1.8


buildscript {
    repositories {
        mavenCentral()
        jcenter()
    }
    dependencies {
        classpath 'com.github.jengelman.gradle.plugins:shadow:1.2.4'
    }
}

repositories {
    mavenCentral()
    maven {
        setUrl('https://repository.cloudera.com/artifactory/cloudera-repos/')
    }
}

/**
 * Update the shadowJar task with necessary properties to compile our fat jar correctly
 */
shadowJar {
    classifier = 'jar-with-dependencies'
    zip64 = true
}

dependencies {

    compile 'org.scala-lang:scala-library:2.11'
    compile 'org.apache.spark:spark-sql_2.11:2.2.0'
    compile 'org.apache.spark:spark-core_2.11:2.2.0'
    compile 'com.github.scopt:scopt_2.11:3.7.0'
    testCompile 'org.scalatest:scalatest_2.11:3.0.4'
    testCompile 'junit:junit:4.12'
}
```

This sets our application up with version 1.0.0.  We've configured gradle to use the java, scala, and shadow plugins. The buildScript() tag specifically let's use bootstrap the gradle shadow plugin location for use in constructing the fat jar. We also have to setup the `repositories` task so that gradle knows where to get the dependencies defined below.

For the shadowJar build option, we tell it to suffix the jar name with `jar-with-dependencies` to clearly denote the artifact is a fat jar. We also tell it to use zip base64 encoding for bytecode to save space. 

Our dependencies are also clearly listed. This will include everything we need for this assignment.

1. Scala lib 2.11
2. Spark core 2.2.0
3. Spark sql 2.2.0
4. A custom Scala opt for command line argument parsing which I'm fond of. It's easy to use. 
5. Scala test and Junit for easy unit testing. 

We're not totally done this step. We also want to define `settings.gradle` for any custom property overrides or variables to use in the build. In our case
we can set `rootProject.name = 'song_plays'` in order to override gradle's default behavior of creating a jar with the basedir name.

In the same location as the `build.gradle` file, create the file `settings.gradle` and add the single line

`rootProject.name = 'song_plays'`

CHECKPOINT: From the scala_spark basedir location with your build script, run `./gradlew clean shadowJar`

It can take a minute or so to initialize the build for the first time, but you should eventually see `BUILD SUCCESSFUL`

Additional note: All the dependency jars for compile and testCompile are already pre-cached in the VM, which is why you don't see any downloads happening. 

### Step 4. Create Scala launcher object. 

Let's create the packages for both our launcher and tests.

From the basedir scala_spark location in the VM, run `mkdir -p src/{main,test}/scala/com/song/plays`.

For now we will just focus on the src/main object and not the tests, but this command will set us up for later. 

Under `src/main/scala/com/song/plays` create the the file `DatasetGen.scala`

Add the lines

```scala
package com.song.plays

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit}

final class FailedValidationError(msg: String) extends RuntimeException

object DatasetGen {

  def main(args: Array[String]): Unit = {}
}
```

This will setup all the imports we'll need, and define a final Error class to throw in the case our job fails minrows validation. 

It also defines a blank main() method which we will fill in later. 

CHECKPOINT: Try running `./gradlew clean build`. You should see `BUILD SUCCESSFUL`.

### Step 5. Setup command line parsing. 

Now we're ready to start adding some useful code to the Spark launcher we just started.

Directly under the `object DatasetGen` definition add a case class for Config. This lets the scopt library automatically create a container object our launcher parameter that we sent on the command-line.

Case classes are useful for definining immutable instances of classes for pattern matching. For more info see [Case Classes](https://docs.scala-lang.org/tour/case-classes.html)

```scala
case class Config(day: String = "",
                  minrows: Int = 0,
                  listeners_path: String = "",
                  spins_path: String = "",
                  dataset_out_path: String = "",
                  analysis_out_path: String = "")
```

We also have to define a parsing method `getParser()` that takes in the array of String arguments from main to construct instances of this Config. Don't worry too much about understanding the details of everything going on in this snippet. I'll try to explain right after you're done entering these lines. 

```scala
/** Parse command line arguments for what's expected.
    * Throw an error if something goes wrong.
    *
    * @param args
    * @return DatasetGenConf
    */
  def getParser(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "3.x")

      opt[String]("day").action( (x, c) =>
        c.copy(day = x) ).text("day is a String property")

      opt[Int]("minrows").action( (x, c) =>
        c.copy(minrows = x) ).text("minrows is an Int property")

      opt[String]("listeners_path").action( (x, c) =>
        c.copy(listeners_path = x) ).text("listeners_path is a String property")

      opt[String]("spins_path").action( (x, c) =>
        c.copy(spins_path = x) ).text("spins_path is a String property")

      opt[String]("dataset_out_path").action( (x, c) =>
        c.copy(dataset_out_path = x) ).text("dataset_out_path is a String property")

      opt[String]("analysis_out_path").action( (x, c) =>
        c.copy(analysis_out_path = x) ).text("analysis_out_path is a String property")
    }

    val cfg : Config = parser.parse(args, Config()) match {
      case Some(config) => config
      case None =>
        println("error trying to parse config arguments")
        Config()
    }
    cfg
  }
```

Here's a quick step-by-step summary of what's going on here. 

1. Create an instance of `OptionParser` with our case class as the type argument. 
2. Then we can define options for how we want to assign the different CLI parameter arguments to variables. There's also a text() field to give a message on what the different arguments mean should the --help option be triggered, or an error occur. 
3. Then call parser.parse() with the arguments string array that initially came from the caller. 
4. The object assigned to `val cfg` will be either the config we parsed, if valid, or for some reason the parser returned None, then we send back an empty config along with an error message. We use pattern matching here to make sure this function never returns null or None.  
5. Either way the last line in the `getParser()` returns a cfg instance back to the caller. 

CHECKPOINT: Let's make sure you entered all the code above correctly and rerun the last checkpoint.

run: `./gradlew clean build` should still produce `BUILD SUCCESSFUL`

### Step 6. Setup functions to transform data frame operations.

Before we get to the actual main() function where we'll initialize a Spark session, let's write a few functions that will help
during the analysis and transformation. You'll see how these all tie together as we continue to work through the workshop. 

First, let's write a function that takes as a parameter a Spark dataframe and keeps all rows that have > 30 for the expected column 'elapsed_seconds'. 

We will use this to remove song plays from our dataset that didn't play for more than 30 seconds. The labels tend not to be interested in these. 

```scala
def filterOnSpinTime(spins: DataFrame) = {
    spins.filter(col("elapsed_seconds") > lit(30))
}
```

Let's also define the function that will group and count the spins dataframe by zipcode and subscription type. This is how we'll produce
our per-zipcode per-subscription type analysis. 

```scala
def countSpinsBySub(deduped_df: DataFrame) = {
    deduped_df.groupBy("fake_zipcode", "subscription_type").
        agg(count("*").as("spins")).
        select("fake_zipcode", "subscription_type", "spins").
        orderBy("fake_zipcode", "subscription_type")
}
```

Finally for this step, let's write a validation function. It will take in 2 integers. One for the number of rows, and another to define what the threshold for a minimum number of acceptable rows should be.
If the number of rows is less than the threshold, we throw an exception. Otherwise exit normally and don't return anything.

```scala
def validate(numRows: Long, minrows: Int) = {
    if (numRows < minrows) {
      val msg = "The job failed validation. " +
        "Number of rows: %d < %d".format(numRows, minrows)
      throw new FailedValidationError(msg)
    }
}
```

This means that unless callers catch the FailedValidationError it could crash the application. This is actually the behavior we want should the validation fail. 
More on this below when you see how we're using this function.

When we get to testing later you'll see why it was a good idea to move the above business logic into their own dedicated functions. 

CHECKPOINT: Run `./gradlew clean build` and the output should still be `BUILD SUCCESSFUL`. 

### Step 7. Setup Spark dataframe operations. 

Now it's time to actually tie everything we just wrote in Scala together into a working application. We will do this in 2 phases just so there's room to explain everything without
a really really long single step. 

First let's take the main() method we already defined and expand the curly braces a bit to start including more logic.

```scala
def main(args: Array[String]): Unit = {

    val cfg = getParser(args)

    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "1")

    val session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
```

This will take the String arguments fed at runtime by Spark-submit into the main class and create a Config instance from them with typed values.  We then setup a Spark session instance
with a single config override fo shuffle.partitions. This is set to 1 because for the purposes of this workshop the datasets are very small, and so we can actually get better performance
turnaround with just 1 dataframe partition during operations. In practice this is often best left alone for Spark to optimize at runtime, but you do occassionally see applications set this manually.

Once we have a reference to the `session` variable we can start doing our operations. 

Underneath this line write the lines to create dataframes from our parquet files at the supplied input paths. 

```scala
val listenersDF = session.read.parquet(cfg.listeners_path)
val spinsDF = filterOnSpinTime(session.read.parquet(cfg.spins_path))
```

This sets up the listeners and spins dataframes from input, and immediately sends the spins dataframe into our elapsed_second filter function. The dataframe assigned to `spinsDF` contains this filtered subset of spins. 

Using the `read.parquet` option makes it really easy to define a dataframe directly from Parquet files, since Parquet files contain an embedded schema. 

Now let's join the listeners dataframe to the spins dataframe and run `distinct()` to remove any duplicates.  

```scala
val joinedDF = spinsDF.join(listenersDF, "fake_listener_id")
val dedupedDF = joinedDF.distinct()
```

We could alternatively run `distinct()` on `spinsDF` and `listenersDF` seperately before joining, but this is less code and the Spark SQL optimizer will hopefully figure out the best way to arrive at this state anyways. No need to overthink this for now unless it becomes a noticable performance problem. (This is a good way to think in general when working on data engineering over distributed systems). 

Now let's do validation on the deduplicated joined dataframe.

```scala
val numRows = dedupedDF.count()
validate(numRows, cfg.minrows)
} // close of main method
```

Note that since count() is considered an action command, this where Spark will start evaluating the previous commands before this and start executing to arrive at the count() state.  Having lots of dataframe count() calls through your application can slow things down, but in our case we have just a single call, and the usefulness far exceeds the small price 
in execution overhead we have to pay. 

Consider what happens if a dataset is malformed and fails validation. The evaluation was all done with transient dataframe operations and there is no cleanup or deletion of any external state which must be done before we can rerun. In practice this is very useful behavior, since it let's us design this job to be fault-tolerant while still be automated. Each run of the job will either A) produce our desired output artifacts and Luigi will know it succeeded, or B) fail validation or some other Spark error and luigi will elevate that exception. The job can be retried later via crontab automatically. Repeated and consistent Luigi errors tend to get data-engineering teams' attention very quickly. 

Moreover, once the count() operation on `dedupedDF` is finished Spark will have cached the contents of that dataframe automatically in memory, meaning if we continue to do transformations after the count() on `dedupedDF` it won't have to reevaluate the execution stages to arrive at the dataframe state again. This is one of the signature characteristics of Spark that has made it so useful for data analysis and transformation. 

CHECKPOINT: Let's run the build command again just to make sure we didn't mistype anything.

Run `./gradlew clean build` and  you should still see `BUILD SUCCESSFUL`

### Step 8. Output Spark results to files. 

In the same main method, first remove the curly brace that has the comment `// close of main method` and continue implementing.

Add the lines:

```scala
dedupedDF.repartition(1).write.
      option("header", "true").
      option("codec", "org.apache.hadoop.io.compress.GzipCodec").
      option("delimiter", "\t").
      option("quote", "\u0000"). // We don't want to quote anything.
      csv(cfg.dataset_out_path)
```

This will take the dataset we just falied, condense it to 1 single gzipped TSV file with a header, and write the output to the path we sent into the application defined at `dataset_out_path`. The music label companies we transfer these files to like to get a single file, even if it's many gigabytes in length and takes longer to produce. 

We also want to define the unicode character for null to quote, just so we don't accidentally quote any fields that are part of the artist names or track titles. This might not be necessary anymore in the latest versions of Spark which don't default to quoting, but it's a habit I've always just kept. Call me crazy.

We also want to call our aggregate analysis function and output the resulting dataframe to a Gzipped TSV file with a header too.

```scala
val spinsPerZipSubDF = countSpinsBySub(dedupedDF)
spinsPerZipSubDF.repartition(1).write.
    option("header", "true").
    option("codec", "org.apache.hadoop.io.compress.GzipCodec").
    option("delimiter", "\t").
    option("quote", "\u0000"). // We don't want to quote anything.
    csv(cfg.analysis_out_path)
} // close of main method
```

Note: Make sure you don't forget to add back the `}` char to close the main method.

Another important aside: For this workshop I didn't include any logging commands, but in practice they are of course very handy to have inside your Spark jobs for debugging and/or shipping off to services like Logstash. 

CHECKPOINT: Let's run the build command again just to make sure we didn't mistype anything.

Run `./gradlew clean build` and  you should still see `BUILD SUCCESSFUL`

Now we're ready to write some tests.

### Step 9. Tests.

Before we actually build the Spark jar and launch it from luigi, we want to be sure the key parts of our Spark business logic are functioning correctly.

Specifically, we want to test our validation function, the elapsed second filtering and the aggregation logic. 

This step will show off another really useful property of Spark. You can test the code without needing any Mocks for Spark itself. 

Under the location `src/test/scala/com/song/plays/`, create the file `DatasetGenTest.scala`

Define the imports

```scala
package com.song.plays
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
```

Now let's define our test suite.

```scala
@RunWith(classOf[JUnitRunner])
class DatasetGenTest extends FunSuite {

  test("count spins by zipcode and subscription type") {
    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "1")

    val session = SparkSession.builder().config(conf).master("local").getOrCreate()
    val spins = session.sparkContext.parallelize(List(
      (90210, "Family"),
      (90210, "Family"),
      (90210, "Ad-Supported")
    ))
    val dedupedDF = session.createDataFrame(spins.map(x => Row.fromTuple(x)),
      StructType(
        Seq(
          StructField(name = "fake_zipcode", dataType = IntegerType, nullable = false),
          StructField(name = "subscription_type", dataType = StringType, nullable = false)
        )
      ))
    val results = DatasetGen.countSpinsBySub(dedupedDF).collect()
    val mappedResults = results map (row => (row.getAs[Int](0),
                                              row.getAs[String](1),
                                              row.getAs[Int](2)))
    assert(mappedResults.toList == List((90210, "Ad-Supported", 1), (90210, "Family", 2)))
    session.stop()
  }
```

This defines a test suite with just one functional test. 

The title of the test says exactly what it's testing. We want to make sure, given a dummy dataframe, that it counts the number of records by zip code and subscription type. 

We can create a local spark session with just `master("local")` on the builder. This tells Spark to run everything directly on the host we're launching the tests from in a somewhat pseudo-distributed mode. People who remember Hadoop remember how annoying this was to setup initially for local testing. Spark makes this orders of magnitude easier. 

We can use the `parallelize` function on the context object to take a local List() of tuples and create an RDD. 

Then we can manually create a dataframe using Row.fromTuple() and supplying a schema that defines the name and type for each column as they appear in the tuple order. 

With this we're able to call `countSpinsBySub()` and see if the behavior is as expected over a real Spark dataframe. 

Calling `collect()` brings the RDD output back into the driver, and then calling `row.getAs` with positions let's use reconstruct the output into tuples that are easier to compare
in assert. For our test case we expect the input to be counted as "2 spins for 90210 Family, and 1 for 90210 Ad supported". 

Now you can start to see why we put the `countSpinsBySub` logic into it's own dedicated function. It makes testing with a dummy dataframe much easier than if everything is bunched together in one giant main() method. 

Let's add a test for the elapsed_seconds filter also. 


```scala
test("filter on spin elapsed_seconds") {
    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "1")

    val session = SparkSession.builder().config(conf).master("local").getOrCreate()
    val spins = session.sparkContext.parallelize(List(
      (2, "My song"),
      (128, "My song2")
    ))
    val dedupedDF = session.createDataFrame(spins.map(x => Row.fromTuple(x)),
      StructType(
        Seq(
          StructField(name = "elapsed_seconds", dataType = IntegerType, nullable = false),
          StructField(name = "track_title", dataType = StringType, nullable = false)
        )
      ))
    val results = DatasetGen.filterOnSpinTime(dedupedDF).collect()
    val mappedResults = results map (row => (row.getAs[Int](0), row.getAs[String](1)))
    assert(mappedResults.toList == List((128, "My song2")))
    session.stop()
  }
```

This uses the same methodology outlined above to build a dataframe with 2 spins, one with 128 elapsed seconds and the other with just 2. The dataframe returned by the
filter function should contain just the spin with 128 seconds.

Let's also add 2 tests to make sure our validation function does what we expect.

```scala
test("validate passes") {
    try {
      DatasetGen.validate(11, 10)
    } catch {
      case c: FailedValidationError => fail("11 is > 10. This should pass")
      case _ : Throwable => fail("Some unrecognized error")
    }

  }

  test("validate fails") {
    try {
      DatasetGen.validate(1, 10)
      fail("1 is < 10. Expected failure")
    } catch {
      case c : FailedValidationError => assert(true)
      case _ : Throwable => fail("Some unrecognized error")
    }
  }
} // end of test suite class.
```

the test `validate passes` calls validate and fails with a specific message if a `FailedValidationError` was thrown. This could be a useful test if anyone later tries to modify that function and causes a regression to the logic. Additionally, this test will fail if any other exception is throw and caught with the wildcare `_`.

the test `validate fails` calls validate and is expected to fail. The only way it passes is if a `FailedValidationError` is thrown from the function. If either and urecognized function is returned or the validate() function returns normally, the test should fail. 

CHECKPOINT: Let's make sure our tests pass. 

Run `./gradlew clean test` and it should show our 4 tests succeeded and the build finishes with `BUILD SUCCESSFUL`

### Step 10. Build the fat jar. 

Now that the tests pass and our program does what we expect, let's produce the jar we intend to launch from our luigi workflow.

A fat jar will contain all of the classpath runtime dependencies our application will need in the JVM. It's normal for this to be quite large.

Run `./gradlew clean test shadowJar`. 

CHECKPOINT: From the same directory you ran gradle in, run `ls build/libs/song_plays-1.0.0-jar-with-dependencies.jar`. You should see a file. 

Congradulations if you've made it this far. This is a lot to process and you should be very proud of yourself.

Now comes the fun part. We are ready to run this against the data we downloaded with our Luigi workflow and look at the output.

See [Putting_it_all_together](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/Putting_it_all_together.md)
