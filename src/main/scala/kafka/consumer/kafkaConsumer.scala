package kafka.consumer

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.File
import com.typesafe.config.ConfigFactory
import kafka.consumer.sparkLoad.ingest

object kafkaConsumer {
  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val configPath = System.getProperty("user.dir") + "\\src\\main\\conf\\"
    val config = ConfigFactory.parseFile(new File(configPath + "consumer.conf"))

    val broker_id = config.getString("broker")
    val group_id = config.getString("groupId")
    val topics = config.getString("topicName")
    val stream_period = config.getString("streamPeriod").toInt

    val topicset = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf = new SparkConf()
      .setMaster("local")
      .setAppName("kafkaConsumer")
    val ssc = new StreamingContext(sparkconf, Seconds(stream_period))

    val message = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicset, kafkaParams)
    )

    val lines = message
      .map(_.value())
      .flatMap(_.split("\n"))

    lines.foreachRDD {
      rdd => ingest(rdd, config) //Method from kafka.consumer.sparkLoad.ingest
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(config.getString("streamTimeout").toInt)
    ssc.stop()
  }
}