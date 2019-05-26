package io.benx.spark.dataframe

import io.benx.spark.utils.ClusterContext

object DataFrame_Tutorial extends App with ClusterContext {
  val dfTags = sparkSession.
    read.
    option("header", true).
    option("inferSchema", true).
    csv("/opt/spark-data/question_tags_10K.csv").
    toDF("id", "tag")

    dfTags.show(10)
}
