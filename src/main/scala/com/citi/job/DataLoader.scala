package com.citi.job

import com.citi.bean.Spark
import com.citi.config.{DataBaseConf, IncrementalRule, TableConf}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataLoader(db: DataBaseConf) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val spark: SparkSession = Spark.getSparkSession
  private var table: TableConf = _

  private def queryDB(sql: String): DataFrame = {
    spark.read.format("jdbc")
      .option("url", db.url)
      .option("user", db.username)
      .option("password", db.password)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("query", sql)
      .load()
  }

  private def queryHive(sql: String): DataFrame = {
    spark.read.format("hive")
      .option("url", db.url)
      .option("user", db.username)
      .option("password", db.password)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("query", sql)
      .load()
  }

  private def saveDataToHive(): Unit = {

  }

  private def getMaxValue(table: String, field: String): String = {
    val sql = s"select max($field) as $field from $table"
    val dataFrame = queryDB(sql)
    val maxValue = dataFrame.collect()(0).get(0).toString
    maxValue
  }

  private def truncateHiveTable(): Unit = {
    val sql = s"TRUNCATE TABLE ${table.targetHiveTable}"
    val dataFrame = queryHive(sql)
  }

  private def setupIncrementalVal(): Unit = {
    for (rule <- table.incrementalRules) {
      val fieldName = s"${db.name}@${rule.joinTable}@${rule.joinFields}"

      var sql = s"SELECT max_value FROM incremental_table WHERE field_name = $fieldName"
      var rows = queryHive(sql).collect()
      if (rows.length != 0) {
        rule.lastVal = rows(0).get(0).toString
      }

      sql = s"SELECT MAX(${rule.joinFields}) FROM ${rule.joinTable}"
      rows = queryHive(sql).collect()
      rule.maxVal = rows(0).get(0).toString
    }
  }

  private def fullLoad(): Unit = {
    truncateHiveTable()
    val sql = s"SELECT * FROM ${loadRule.sourceTableName}"
    val dataFrame = queryDB(sql)
  }

  private def updateIncrementalTable(): Unit = {
    for (rule <- table.incrementalRules) {
      val fieldName = s"${db.name}@${rule.joinTable}@${rule.joinFields}"
      val sql = s"UPDATE ${incremental_table} SET value = ${rule.maxVal} WHERE field_name = ${fieldName}"

    }
  }

  private def incrementalLoad(): Unit = {
    setupIncrementalVal()
    val sql = s"SELECT * FROM ${table.name} WHERE "

    for (rule <- table.incrementalRules) {
      val whereCondition = ""
      if (rule.lastVal) {
        whereCondition.addString(s"${rule.incrementalField} > ${rule.lastVal} AND ")
      }
      whereCondition.addString(s"${rule.incrementalField} <= ${rule.maxVal}")
      sql.addString(s" ($whereCondition) ")

      if (rule != table.incrementalRules.last) {
        sql.addString(" AND ")
      }
    }

    val dataFrame = queryDB(sql)
    // 写入Hive
    updateIncrementalTable()
  }

  def loadData(tableConf: TableConf): Unit = {
    if (tableConf == null) {
      logger.error("Table Detail is null")
      return
    }
    table = tableConf
    if (!table.incrementalLoad) {
      fullLoad(table)
      return
    }
    incrementalLoad(table)
    table = null
  }
}


