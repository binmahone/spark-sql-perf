package com.databricks.spark.sql.perf

import org.apache.spark.sql.SparkSession

object MhbResultReader {
  def main(args: Array[String]): Unit = {}
  val spark = SparkSession
    .builder()
    .appName("Parquet Read Perf")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "2000")
    .getOrCreate()

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.types._
  import spark.implicits._

  spark.read
    .json("/Users/hongbin.ma/code/spark-sql-perf/tpcds-result.json")
    .createOrReplaceTempView("sqlPerformanceCompact")

  val timestampWindow = Window.partitionBy("sparkVersion").orderBy($"timestamp".desc)

  class RecentRuns(prefix: String, inputFunction: DataFrame => DataFrame) {
    val inputData = spark
      .table("sqlPerformanceCompact")
      .where($"tags.runtype" === "benchmark")
      .withColumn("sparkVersion", $"configuration.sparkVersion")
      .withColumn("startTime", from_unixtime($"timestamp" / 1000))
      .withColumn("database", $"tags.database")

    val recentRuns = inputFunction(inputData)
      .withColumn("runId", dense_rank().over(timestampWindow))
      .withColumn("runId", -$"runId")
      .filter($"runId" >= -10)

    val runtime =
      (col("result.analysisTime") + col("result.optimizationTime") + col("result.planningTime") + col(
        "result.executionTime")).as("runtime")
    val baseData = recentRuns
      .withColumn("result", explode($"results"))
      .withColumn("day", concat(month($"startTime"), lit("-"), dayofmonth($"startTime")))
      .withColumn("runtimeSeconds", runtime / 1000)
      .withColumn("queryName", $"result.name")

    baseData.createOrReplaceTempView(s"${prefix}_baseData")

  }

  val tpcds1500 = new RecentRuns("tpcds1500", _.filter($"database" === "tpcds_sf100_parquet"))

  spark
    .sql("select iteration,queryName,runtimeSeconds from tpcds1500_baseData")
    .write
    .mode("overwrite")
    .option("header", true)
    .csv("/tmp/tpcds_sf100_parquet_result.csv")
}
