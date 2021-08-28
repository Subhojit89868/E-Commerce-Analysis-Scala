package kafka.producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Utils.sleep
import org.apache.log4j._
import scala.io.Source
import java.io.File
import com.typesafe.config.ConfigFactory

object kafkaProducer {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val configPath = System.getProperty("user.dir") + "\\src\\main\\conf\\"
    val config = ConfigFactory.parseFile(new File(configPath + "producer.conf"))

    val broker_id = config.getString("broker")
    val topics = config.getString("topicName")
    val rec_delay = config.getString("recordPublishDelay").toInt

    val kafkaParams: Properties = {
      val props = new Properties()
      props.put("bootstrap.servers", broker_id)
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      props
    }

    val data = Source.fromFile(config.getString("srcPath")).getLines.drop(1)

    val producer = new KafkaProducer[String, String](kafkaParams)
    for(lines <- data){
      producer.send(new ProducerRecord[String, String](topics, lines))
      sleep(rec_delay)
    }
    producer.close()
  }
}