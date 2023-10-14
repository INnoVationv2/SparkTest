package com.citi.job

import com.citi.bean.Spark
import com.citi.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class Job {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val spark: SparkSession = Spark.getSparkSession

  private def loadData(): Unit = {
    //    spark.sql("select * from test").show()

    //    //    val df = spark.read.json("/Users/liuyu/info.json")
    //    //    df.createTempView("people")
    //    //    df.printSchema()
    //    //    spark.sql("select * from people").show()
    //    jdbcDF.printSchema()
    //    jdbcDF.show()
    //    val row = jdbcDF.collect()(0)
    //    val value = row.get(0).toString
    //    println(value)
    //    println(value.getClass)
    //    spark.sql("select * from test").show()
    //    val reader2 = spark.read
    //      .format("jdbc")
    //      .option("url", "jdbc:oracle:thin:@192.168.1.17:1521:helowin")
    //      //      .option("dbtable", "test.item")
    //      .option("user", "test")
    //      .option("password", "test")
    //      .option("driver", "oracle.jdbc.driver.OracleDriver")
    //      .option("query", s"select * from item where item_id = 5")
    //      .load()
    //    reader2.printSchema()
    //    reader2.show()
    //    val rows = reader2.collect()
    //    println(rows.length)
    //    print(jdbcDF)
  }

  def start(): Unit = {
    val config: Config = Config.getConfig
    for (instance <- config.instances) {
      val dataLoader = new DataLoader(instance)
      for (table <- config.tables) {
        dataLoader.loadData(table)
      }
    }
  }
}
