// Databricks notebook source
// MAGIC %md 
// MAGIC This notebook runs spark-sql-perf TPCDS benchmark on and saves the result.

// COMMAND ----------

// Database to be used:
// TPCDS Scale factor
val scaleFactor = "100"
// If false, float type will be used instead of decimal.
val useDecimal = true
// If false, string type will be used instead of date.
val useDate = true
// name of database to be used.
val databaseName = s"tpcds_sf${scaleFactor}_parquet"// +
 // s"""_${if (useDecimal) "with" else "no"}decimal""" +
//  s"""_${if (useDate) "with" else "no"}date""" //+
//  s"""_${if (filterNull) "no" else "with"}nulls"""

val iterations = 3 // how many times to run the whole set of queries.

val timeout = 60 // timeout in hours

val query_filter = Seq() // Seq() == all queries
//val query_filter = Seq("q1-v2.4", "q2-v2.4") // run subset of queries
val randomizeQueries = false // run queries in a random order. Recommended for parallel runs.

// detailed results will be written as JSON to this location.
val resultLocation = "s3a://mhb-dev-bucket/spark-sql-perf/tpcds-photon-results"

// COMMAND ----------

// Spark configuration
spark.conf.set("spark.sql.broadcastTimeout", "10000") // good idea for Q14, Q88.

// ... + any other configuration tuning

// COMMAND ----------

sql(s"use $databaseName")

// COMMAND ----------

import com.databricks.spark.sql.perf.tpcds.TPCDS

val tpcds = new TPCDS (sqlContext = sqlContext)
def queries = {
  val filtered_queries = query_filter match {
    case Seq() => tpcds.tpcds2_4Queries
    case _ => tpcds.tpcds2_4Queries.filter(q => query_filter.contains(q.name))
  }
  if (randomizeQueries) scala.util.Random.shuffle(filtered_queries) else filtered_queries
}
val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  tags = Map("runtype" -> "benchmark", "database" -> databaseName, "scale_factor" -> scaleFactor))

println(experiment.toString)
experiment.waitForFinish(timeout*60*60)

// COMMAND ----------

displayHTML(experiment.html)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit, substring}
val summary = experiment.getCurrentResults
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)

display(summary)