// Databricks notebook source
val SASKey = ""
spark.conf.set("fs.azure.account.key.sadetest.dfs.core.windows.net",SASKey)

// COMMAND ----------

import org.apache.spark.sql.functions._
val df = spark.read.format("csv").option("Header", true).load("abfss://data@sadetest.dfs.core.windows.net/DOB_Job_Application_Filings.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

// COMMAND ----------

val column = "Borough"

// COMMAND ----------

sc.setJobDescription("Spill Listener")
// Stolen the Apache Spark test suite, TestUtils
// https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TestUtils.scala

class SpillListener extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler.{SparkListenerTaskEnd,SparkListenerStageCompleted}
  import org.apache.spark.executor.TaskMetrics
  import scala.collection.mutable.{HashMap,ArrayBuffer}

  private val stageIdToTaskMetrics = new HashMap[Int, ArrayBuffer[TaskMetrics]]
  private val spilledStageIds = new scala.collection.mutable.HashSet[Int]

  def numSpilledStages: Int = synchronized {spilledStageIds.size}
  def reset(): Unit = synchronized { spilledStageIds.clear }
  def report(): Unit = synchronized { println(f"Spilled Stages: ${numSpilledStages}%,d") }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {stageIdToTaskMetrics.getOrElseUpdate(taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics}

  override def onStageCompleted(stageComplete: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageComplete.stageInfo.stageId
    val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
    val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
    if (spilled) spilledStageIds += stageId
  }
}
val spillListener = new SpillListener()
sc.addSparkListener(spillListener)

// COMMAND ----------

sc.setJobDescription("Baseline")
spillListener.reset()

val df = spark.read.format("csv").option("Header", true).load("abfss://data@sadetest.dfs.core.windows.net/DOB_Job_Application_Filings.csv")

df.orderBy(column)                             // Some wide transformation
 .write.format("noop").mode("overwrite").save() // Execute a noop write to test

spillListener.report()

// COMMAND ----------

sc.setJobDescription("Union")
spillListener.reset()

var uniondf = df  
     .union(df) 
     .union(df) 
     .union(df) 
     .union(df) 
     .union(df) 
     .orderBy(column)                               // Some wide transformation
     .write.format("noop").mode("overwrite").save()   // Execute a noop write to test

spillListener.report()

// COMMAND ----------

sc.setJobDescription("Explode")
spillListener.reset()

val data = Range.inclusive(1,50).toArray

val count = df                                   
  .withColumn("stuff", lit(data))                
  .select($"*", explode($"stuff"))              
  .distinct()                                    // Some wide transformation
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

spillListener.report()
