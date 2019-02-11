package com.song.plays
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DatasetGenTest extends FunSuite {

  test("analysis") {
    val session = SparkSession.builder().master("local").getOrCreate()
    val spins = session.sparkContext.parallelize(List(
      (90210, "Family"),
      (90210, "Family"),
      (90210, "Ad-Supported")
    ))
    val deduped_df = session.createDataFrame(spins.map(x => Row.fromTuple(x)),
      StructType(
        Seq(
          StructField(name = "fake_zipcode", dataType = IntegerType, nullable = false),
          StructField(name = "subscription_type", dataType = StringType, nullable = false)
        )
      ))
    val results = DatasetGen.count_spins_by_zip_sub(deduped_df).collect()
    val mapped_results = results map (row => (row.getAs[Int](0),
                                              row.getAs[String](1),
                                              row.getAs[Int](2)))
    assert(mapped_results.toList == List((90210, "Ad-Supported", 1), (90210, "Family", 2)))
    session.stop()
  }

}
