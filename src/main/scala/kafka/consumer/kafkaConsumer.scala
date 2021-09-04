package kafka.consumer

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.StreamingContext
import com.typesafe.config.Config
import org.apache.spark.streaming.dstream.InputDStream

object kafkaConsumer {
  def consumer(ssc:StreamingContext, config:Config): InputDStream[ConsumerRecord[String, String]] = {

    val broker_id   = config.getString("broker")
    val group_id    = config.getString("groupId")
    val topics      = config.getString("topicName")
    val topicSet    = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )

    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )
  }
}