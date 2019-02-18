package com.song.plays
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


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

}
