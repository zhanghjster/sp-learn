package io.benx.spark.localfile

import io.benx.spark.utils.LocalContext

object LocalObject extends App with LocalContext{
  println(sparkSession.
    sparkContext.
    textFile("src/main/resources/question_tags_10K.csv")
    .map(s => {
      val kvPairs = s.split(",")
      (kvPairs(0), 1)
    }).
    reduceByKey((a, b) => a+b).
    sortByKey(false).
    top(10).
    mkString("\n")
  )
}
