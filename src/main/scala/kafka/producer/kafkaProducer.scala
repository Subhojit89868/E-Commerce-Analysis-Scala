package kafka.producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Utils.sleep
import org.apache.log4j._
import scala.io.Source

object kafkaProducer {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val broker_id = "localhost:9092"
    val topics = "test"

    val kafkaParams: Properties = {
      val props = new Properties()
      props.put("bootstrap.servers", broker_id)
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      props
    }

    val data = Source.fromFile("data/input/sample.txt").getLines.drop(1)

    val producer = new KafkaProducer[String, String](kafkaParams)
    for(lines <- data){
      producer.send(new ProducerRecord[String, String](topics, lines))
      sleep(1000)
    }
    producer.close()
  }
}