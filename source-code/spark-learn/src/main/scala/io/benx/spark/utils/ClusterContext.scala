package io.benx.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait ClusterContext {
  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("spark://spark-master:7077")
    .set("spark.cores.max", "1")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}

trait LocalContext {
  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}
