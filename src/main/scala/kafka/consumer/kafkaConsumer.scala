package kafka.consumer

import org.apache.spark.{SparkConf, sql}
import org.apache.kafka.clients.consumer._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j._

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

    val lines = message.map(_.value()).flatMap(_.split(" "))
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}