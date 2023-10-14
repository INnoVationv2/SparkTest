package com.citi.bean

import org.apache.spark.sql.SparkSession

object Spark {
  private var sparkSession: SparkSession = _

  def getSparkSession: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession.builder()
        .master("local[*]")
        .appName("mySpark")
        .enableHiveSupport()
        .getOrCreate()
    }
    sparkSession
  }
}
