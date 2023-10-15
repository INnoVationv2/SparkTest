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
                     loadType: String,
                     var incrementalFields: List[IncrementalField],
                     var joinRule: JoinRule,
                     partition: Boolean,
                     var partitionColumns: List[String]) {
  def this(confMap: Map[String, Any]) {
    this(
      name = confMap("name").asInstanceOf[String],
      loadType = confMap("load_type").asInstanceOf[String],
      incrementalFields = null,
      joinRule = null,
      partition = confMap("partition").asInstanceOf[Boolean],
      partitionColumns = null
    )

    if (loadType == "incremental_load") {
      val incrementalRuleConfigs = confMap("incremental_fields").asInstanceOf[util.List[String]].asScala
      incrementalFields = incrementalRuleConfigs.map(field => {
        IncrementalField(field)
      }).toList
    }

    if (loadType == "join_with_other_table") {
      val joinRuleConf = confMap("join_rule").asInstanceOf[util.Map[String, String]].asScala
      joinRule = new JoinRule(joinRuleConf.toMap)
    }

    if (partition) {
      partitionColumns = confMap("partition_columns").asInstanceOf[util.List[String]].asScala.toList
    }
  }
}

class BaseIncrementalRule() {
  var lastVal: String = _
  var maxVal: String = _
}

case class IncrementalField(field: String) extends BaseIncrementalRule

case class JoinRule(incrementalField: String,
                    joinField: String,
                    joinTable: String,
                    joinTableIncrementalField: String) extends BaseIncrementalRule {
  def this(confMap: Map[String, String]) {
    this(
      incrementalField = confMap("incremental_field").asInstanceOf[String],
      joinField = confMap("join_field").asInstanceOf[String],
      joinTable = confMap("join_table"),
      joinTableIncrementalField = confMap("join_table_incremental_field").asInstanceOf[String]
    )
  }
}