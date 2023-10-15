package com.citi.config

import org.yaml.snakeyaml.Yaml

import java.util
import scala.jdk.CollectionConverters._

case class Config(instances: List[DataBaseConf], tables: List[TableConf])

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

    val instances = loadInstances(yamlData)
    val tables = loadTables(yamlData)

    Config(instances, tables)
  }

  private def loadInstances(yamlData: collection.mutable.Map[String, Any]): List[DataBaseConf] = {
    val instanceConfigs = yamlData.getOrElse("oracles", List.empty).asInstanceOf[util.List[Any]].asScala
    instanceConfigs.map { instanceConf =>
      val instanceConfMap = instanceConf.asInstanceOf[util.Map[String, String]].asScala.toMap
      new DataBaseConf(instanceConfMap)
    }.toList
  }

  private def loadTables(yamlData: collection.mutable.Map[String, Any]): List[TableConf] = {
    val tableConfigs = yamlData.getOrElse("tables", List.empty).asInstanceOf[util.List[Any]].asScala
    tableConfigs.map { tableConfig =>
      val tableConfMap = tableConfig.asInstanceOf[util.Map[String, Any]].asScala.toMap
      new TableConf(tableConfMap)
    }.toList
  }
}

case class DataBaseConf(name: String, url: String, username: String, password: String) {
  def this(confMap: Map[String, String]) {
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
                     partition: Boolean) {
  var incrementalFields: List[IncrementalField] = _
  var joinRule: JoinRule = _
  var partitionColumns: List[String] = _

  def this(confMap: Map[String, Any]) {
    this(
      name = confMap("name").asInstanceOf[String],
      loadType = confMap("load_type").asInstanceOf[String],
      partition = confMap("partition").asInstanceOf[Boolean],
    )

    if (loadType == "incremental_load") {
      val incrementalRuleConfigs = confMap("incremental_fields").asInstanceOf[util.List[String]].asScala
      incrementalFields = incrementalRuleConfigs.map(IncrementalField).toList
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
                    joinTableIncrementalField: String
                   ) extends BaseIncrementalRule {
  def this(confMap: Map[String, String]) {
    this(
      incrementalField = confMap("incremental_field"),
      joinField = confMap("join_field"),
      joinTable = confMap("join_table"),
      joinTableIncrementalField = confMap("join_table_incremental_field")
    )
  }
}