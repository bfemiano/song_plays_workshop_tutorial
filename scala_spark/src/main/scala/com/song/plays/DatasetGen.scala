package com.song.plays

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit}

final class FailedValidationError(msg: String) extends RuntimeException

object DatasetGen {

  case class Config(day: String = "",
                    minrows: Int = 0,
                    listeners_path: String = "",
                    spins_path: String = "",
                    dataset_out_path: String = "",
                    analysis_out_path: String = "")


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

  def filterOnSpinTime(spins: DataFrame) = {
    spins.filter(col("elapsed_seconds") > lit(30))
  }

  def countSpinsBySub(deduped_df: DataFrame) = {
    deduped_df.groupBy("fake_zipcode", "subscription_type").
      agg(count("*").as("spins")).
      select("fake_zipcode", "subscription_type", "spins").
      orderBy("fake_zipcode", "subscription_type")
  }

  def validate(numRows: Long, minrows: Int) = {
    if (numRows < minrows) {
      val msg = "The job failed validation. " +
        "Number of rows: %d < %d".format(numRows, minrows)
      throw new FailedValidationError(msg)
    }
  }

  def main(args: Array[String]): Unit = {

    val cfg = getParser(args)

    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "1")

    val session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val listenersDF = session.read.parquet(cfg.listeners_path)
    val spinsDF = filterOnSpinTime(session.read.parquet(cfg.spins_path))
    val joinedDF = spinsDF.join(listenersDF, "fake_listener_id")
    val dedupedDF = joinedDF.distinct()
    val numRows = dedupedDF.count()
    validate(numRows, cfg.minrows)

    dedupedDF.repartition(1).write.
      option("header", "true").
      option("codec", "org.apache.hadoop.io.compress.GzipCodec").
      option("delimiter", "\t").
      option("quote", "\u0000"). // We don't want to quote anything.
      csv(cfg.dataset_out_path)

    val spinsPerZipSubDF = countSpinsBySub(dedupedDF)
    spinsPerZipSubDF.repartition(1).write.
      option("header", "true").
      option("codec", "org.apache.hadoop.io.compress.GzipCodec").
      option("delimiter", "\t").
      option("quote", "\u0000"). // We don't want to quote anything.
      csv(cfg.analysis_out_path)
  }
}
