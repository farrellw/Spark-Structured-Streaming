package com.farrellw.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StreamFromKafka {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.208.65.122:9092,34.68.16.1:9092")
      .option("subscribe", "orders")
      .option("startingOffsets", "earliest")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }
}
