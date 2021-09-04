import java.io.File
import com.typesafe.config.ConfigFactory
import kafka.consumer.kafkaConsumer.consumer
import kafka.consumer.sparkLoad.ingest
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object mainIngestion{
  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val configPath = System.getProperty("user.dir") + "\\src\\main\\conf\\"
    val config = ConfigFactory.parseFile(new File(configPath + "consumer.conf"))

    val sparkconf = new SparkConf()
      .setMaster("local")
      .setAppName("kafkaConsumer")

    val ssc = new StreamingContext(sparkconf, Seconds(config.getString("streamPeriod").toInt))
    val message = consumer(ssc, config) // Method from kafka.consumer.kafkaConsumer.consumer

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