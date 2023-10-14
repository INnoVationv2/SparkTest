package com.citi.config

import org.yaml.snakeyaml.Yaml

import java.util
import scala.jdk.CollectionConverters._


object Config {
  private var config: Config = _

  def getConfig: Config = {
    if (config == null) {
      config = loadConfig()
    }
    config
  }

  private def loadConfig(): Config = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("config.yml")
    val yamlData = yaml.load(inputStream).asInstanceOf[util.Map[String, Any]].asScala

    // load instance config
    val instanceConfigs = yamlData("oracles").asInstanceOf[util.List[Any]].asScala
    val instanceList = instanceConfigs.map(instanceConf => {
      val instanceConfMap = instanceConf.asInstanceOf[util.Map[String, String]].asScala.toMap
      new DataBaseConf(instanceConfMap)
    }).toList

    // load table config
    val tableConfigs = yamlData("tables").asInstanceOf[util.List[Any]].asScala
    val tableList = tableConfigs.map(tableConfig => {
      val tableConfMap = tableConfig.asInstanceOf[util.Map[String, Any]].asScala.toMap
      new TableConf(tableConfMap)
    }).toList

    Config(instances = instanceList, tables = tableList)
  }
}

case class Config(instances: List[DataBaseConf], tables: List[TableConf])

case class DataBaseConf(name: String,
                        url: String,
                        username: String,
                        password: String) {
  def this(confMap: Map[String, String]) = {
    this(
      name = confMap("name"),
      url = confMap("url"),
      username = confMap("username"),
      password = confMap("password")
    )
  }
}

case class TableConf(name: String,
                     incrementalLoad: Boolean,
                     var incrementalRules: List[IncrementalRule],
                     targetHiveTable: String,
                     targetSchema: String,
                     partition: Boolean,
                     var partitionColumns: List[String]) {
  def this(confMap: Map[String, Any]) {
    this(
      name = confMap("name").asInstanceOf[String],
      incrementalLoad = confMap("incremental_load").asInstanceOf[Boolean],
      incrementalRules = null,
      targetHiveTable = confMap("target_hive_table").asInstanceOf[String],
      targetSchema = confMap("target_schema").asInstanceOf[String],
      partition = confMap("partition").asInstanceOf[Boolean],
      partitionColumns = null
    )

    if (incrementalLoad) {
      val incrementalRuleConfigs = confMap("incremental_rule").asInstanceOf[util.List[util.Map[String, String]]].asScala
      incrementalRules = incrementalRuleConfigs.map(rule => {
        new IncrementalRule(rule.asScala.toMap)
      }).toList
    }

    if (partition) {
      partitionColumns = confMap("partition_column").asInstanceOf[util.List[String]].asScala.toList
    }
  }
}

case class IncrementalRule(incrementalField: String,
                               joinTable: String,
                               joinFields: String) {
  var lastVal: String = _
  var maxVal: String = _

  def this(confMap: Map[String, String]) = {
    this(
      incrementalField = confMap("incremental_field"),
      joinTable = confMap("join_table"),
      joinFields = confMap("join_field")
    )
  }
}