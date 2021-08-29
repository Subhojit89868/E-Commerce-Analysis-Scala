package kafka.consumer

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, dayofmonth, month, year}
import resource.ingestSchema

object sparkLoad {
  def ingest(rdd:RDD[String], config:Config): Unit ={
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    val data = rdd
      .map(_.split(",").to[List])
      .map(line => row(line))
    val schema = ingestSchema(config) //Method from resource.ingestSchema
    val rawDF = spark.createDataFrame(data, schema)

    val fin = rawDF
      .withColumn("etl_ts", current_timestamp)
      .withColumn("year", year(current_timestamp))
      .withColumn("month", month(current_timestamp))
      .withColumn("day", dayofmonth(current_timestamp))

    fin.write
      .partitionBy("year","month","day")
      .mode("append")
      .csv("data/output/")
  }
  def row(column:List[String]): Row ={
    if(column.nonEmpty){
      var columnAppend = Seq(column.head)
      for(i <- 1 until column.length){
        columnAppend = columnAppend ++ Seq(column(i))
      }
      Row.fromSeq(columnAppend)
    }
    else
      Row()
  }
}