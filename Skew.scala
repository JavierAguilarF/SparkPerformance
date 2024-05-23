// Databricks notebook source
val SASKey = ""
spark.conf.set("fs.azure.account.key.sadetest.dfs.core.windows.net",SASKey)

// COMMAND ----------

val df_original = spark.read.format("csv").option("Header", true).load("abfss://data@sadetest.dfs.core.windows.net/DOB_Job_Application_Filings.csv")
val unionDF2 = df_original.union(df_original)
val unionDF4 = unionDF2.union(unionDF2)
val unionDF8 = unionDF4.union(unionDF4)
val df = unionDF8.union(unionDF8)

val columnNames = Seq("Job Type","Job Status Descrp","Proposed Occupancy")
val df2 = df_original.select(columnNames.head, columnNames.tail: _*)
val df2_sub = df2.groupBy("proposed occupancy").count.orderBy($"count")

val df_hint = unionDF8.union(unionDF8).hint("skew","proposed occupancy") //skewhint df

// COMMAND ----------

display(df.groupBy("proposed occupancy").count.orderBy($"count"))

// COMMAND ----------

sc.setJobDescription("Baseline")

//Ensure AQE is disabled
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

df.join(df2_sub, df("proposed occupancy") === df2_sub("proposed occupancy")).write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Skew Hint")
//Ensure AQE is disabled
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

df_hint.join(df2_sub, df("proposed occupancy") === df2_sub("proposed occupancy")).write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("AQE")
//Ensure AQE is enabled
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)

df.join(df2_sub, df("proposed occupancy") === df2_sub("proposed occupancy")).write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Salted Join I")
//Ensure AQE is disabled
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

val skewFactor = 4000
val saltDF = spark.range(skewFactor).toDF("salt")

import org.apache.spark.sql.functions._
val partitions = Math.ceil(865 / 128d).toInt
val df2SaltedDF = df2_sub.repartition(partitions).crossJoin(saltDF).withColumn("salted_id",concat($"proposed occupancy", lit("_"), $"salt")).drop("salt")

val dfSaltedDF = df.withColumn("salt",(lit(skewFactor) * rand()).cast("int")).withColumn("salted_id",concat($"proposed occupancy", lit("_"), $"salt")).drop("salt")

dfSaltedDF.join(df2SaltedDF, dfSaltedDF("salted_id") === df2SaltedDF("salted_id")).write.format("noop").mode("overwrite").save()
