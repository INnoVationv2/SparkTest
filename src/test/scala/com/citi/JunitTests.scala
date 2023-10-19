package com.citi

//import com.citi.job.Job
import com.citi.bean.Spark.getSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.junit.Test


class JunitTests{
  @Test
  def testIncremental(): Unit = {
    val spark: SparkSession = getSparkSession

    val schema = new StructType()
      .add(StructField("id", IntegerType, nullable = false))
      .add(StructField("name", StringType, nullable = false))
      .add(StructField("age", IntegerType, nullable = false))
    var rdd = spark.sparkContext.parallelize(Seq(Row(1, "Alice", 18), Row(2, "Bob", 20)))
    var df = spark.createDataFrame(rdd, schema)

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.h2.Driver")
    prop.setProperty("user", "sa")
    prop.setProperty("password", "sa")

    df.write.mode(SaveMode.Overwrite).jdbc("jdbc:h2:~/test", "item", prop)

    spark.read.format("jdbc")
      .option("url", "jdbc:h2:~/test")
      .option("user", "sa")
      .option("password", "sa")
      .option("driver", "org.h2.Driver")
      .option("query", "SELECT * FROM item")
      .load().show()

    rdd = spark.sparkContext.parallelize(Seq(Row(1, "Alice", 22), Row(2, "Jenny", 20)))
    df = spark.createDataFrame(rdd, schema)

    df.write.mode(SaveMode.Overwrite).jdbc("jdbc:h2:~/test", "item", prop)
    val df2 = spark.read.format("jdbc")
      .option("url", "jdbc:h2:~/test")
      .option("user", "sa")
      .option("password", "sa")
      .option("driver", "org.h2.Driver")
      .option("query", "SELECT * FROM item")
      .load()

    if (df.except(df2).count() == 0 && df2.except(df).count() == 0){
      println("Equal")
    } else {
      println("False")
    }
  }
}
