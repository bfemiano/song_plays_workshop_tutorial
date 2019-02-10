package com.song.plays

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object DatasetGen {

  case class Config(day: String = "",
                    listeners_path: String = "",
                    spins_path: String = "",
                    out_path: String = "")

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

      opt[String]("out_path").action( (x, c) =>
        c.copy(out_path = x) ).text("out_path is a String property")
    }

    val cfg : Config = parser.parse(args, Config()) match {
      case Some(config) => config
      case None =>
        println("error trying to parse config arguments")
        Config()
    }
    cfg
  }

  def main(args: Array[String]): Unit = {

    val cfg = getParser(args)

    val conf = new SparkConf()
    conf.set("spark.yarn.executor.memoryOverhead", "2048")
    conf.set("spark.sql.shuffle.partitions", "2048")

    val session = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    println("RUNNING JAR")
    println(cfg.day)
  }
}
