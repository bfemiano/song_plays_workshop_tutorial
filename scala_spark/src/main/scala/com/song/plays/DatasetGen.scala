package com.song.plays

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions.count

object DatasetGen {

  case class Config(day: String = "",
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

  def analysis(deduped_df: DataFrame) = {
    deduped_df.groupBy("fake_zipcode", "subscription_type").
      agg(count("*").as("spins")).
      select("fake_zipcode", "subscription_type", "spins").
      orderBy("fake_zipcode", "subscription_type")
  }

  def main(args: Array[String]): Unit = {

    val cfg = getParser(args)

    val session = SparkSession
      .builder()
      .getOrCreate()

    val listeners_df = session.read.parquet(cfg.listeners_path)
    val spins_df = session.read.parquet(cfg.spins_path)
    val joined_df = spins_df.join(listeners_df, "fake_listener_id")
    val deduped_df = joined_df.distinct()

    deduped_df.repartition(1).write.
      option("header", "true").
      option("codec", "org.apache.hadoop.io.compress.GzipCodec").
      option("delimiter", "\t").
      option("quote", "\u0000"). // We don't want to quote anything.
      csv(cfg.dataset_out_path)

    val spins_per_zip_subtype_df = analysis(deduped_df)
    spins_per_zip_subtype_df.repartition(1).write.
      option("header", "true").
      option("codec", "org.apache.hadoop.io.compress.GzipCodec").
      option("delimiter", "\t").
      option("quote", "\u0000"). // We don't want to quote anything.
      csv(cfg.analysis_out_path)
  }
}
