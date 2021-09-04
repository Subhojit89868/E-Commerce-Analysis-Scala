package kafka.consumer

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import resource.ingestSchema

object sparkLoad {

  def ingest(rdd:RDD[String], config:Config): Unit ={

    val spark = SparkSession
      .builder
      .config(rdd.sparkContext.getConf)
      .getOrCreate()
    val data  = rdd
      .map(_.split(",").to[List])
      .map(line => transform(line))

    val userSchema = ingestSchema(config) //Method from resource.ingestSchema
    val rawDF      = spark.createDataFrame(data, userSchema)

    rawDF.write
      .mode("append")
      .parquet("data/output/tmp/")
  }

  def transform(line:List[String]): Row ={
    if(line.nonEmpty){
      var lineAppend = Seq(line.head)
      for(i <- 1 until line.length){
        lineAppend   = lineAppend ++ Seq(line(i))
      }
      Row.fromSeq(lineAppend)
    }
    else
      Row()
  }
}