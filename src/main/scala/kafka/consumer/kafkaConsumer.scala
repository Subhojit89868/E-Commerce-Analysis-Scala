package kafka.consumer

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{current_timestamp, dayofmonth, month, year}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaConsumer {
  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val broker_id = "localhost:9092"
    val group_id = "1"
    val topics = "test"

    val topicset = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf = new SparkConf().setMaster("local").setAppName("kafkaConsumer")
    val ssc = new StreamingContext(sparkconf, Seconds(2))

    val message = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicset, kafkaParams)
    )

    val lines = message.map(_.value()).flatMap(_.split("\n"))
    val schema = new StructType()
      .add("id", StringType, nullable = false)
      .add("mod_ts", StringType, nullable = false)
      .add("amount", StringType, nullable = false)
      .add("status", StringType, nullable = false)

    lines.foreachRDD { rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val data = rdd
        .map(_.split(",").to[List])
        .map(line => Row(line(0), line(1), line(2), line(3)))

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

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000)
    ssc.stop()
  }
}