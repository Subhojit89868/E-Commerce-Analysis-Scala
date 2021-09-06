import java.io.File
import java.time.LocalDateTime
import com.typesafe.config.ConfigFactory
import kafka.consumer.kafkaConsumer.consumer
import kafka.consumer.sparkLoad.ingest
import org.apache.commons.io.FileUtils
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object mainIngestion{

  def main(args: Array[String]):

    Logger.getLogger("org").setLevel(Level.ERROR)

    println(LocalDateTime.now() + " : Sourcing Config file.")
    val configPath = System.getProperty("user.dir") + "\\src\\main\\conf\\"
    val config     = ConfigFactory.parseFile(new File(configPath + "consumer.conf"))
    println(LocalDateTime.now() + " : Sourced Config Object : " + config)

    val sparkconf  = new SparkConf()
      .setMaster("local")
      .setAppName("kafkaConsumer")
    val ssc        = new StreamingContext(sparkconf, Seconds(config.getString("streamPeriod").toInt))

    val message    = consumer(ssc, config) // Method from kafka.consumer.kafkaConsumer.consumer
    val lines      = message
      .map(_.value())
      .flatMap(_.split("\n"))

    lines.foreachRDD {
      rdd => println(LocalDateTime.now() + " : Storing Raw messages in tmp directory.")
        ingest(rdd, config) //Method from kafka.consumer.sparkLoad.ingest
    }

    println(LocalDateTime.now() + " : Starting Streaming Context.")
    ssc.start()
    ssc.awaitTerminationOrTimeout(config.getString("streamTimeout").toInt)
    println(LocalDateTime.now() + " : Streaming Context timed out, hence stopping.")
    ssc.stop()

    val spark = SparkSession
      .builder
      .master("local")
      .getOrCreate()
    println(LocalDateTime.now() + " : Writing final data into target directory")
    write(spark)
  }

  def write(spark: SparkSession): Unit ={
    spark.read
      .parquet("data/output/tmp/*.parquet")
      .withColumn("etl_ts", current_timestamp)
      .withColumn("year", date_format(current_timestamp, "yyyy"))
      .withColumn("month", date_format(current_timestamp, "MM"))
      .withColumn("day", date_format(current_timestamp, "dd"))
      .orderBy("id")
      .repartition(1)
      .write
      .mode("append")
      .partitionBy("year","month","day")
      .parquet("data/output/partition/")
    println(LocalDateTime.now() + " : Deleting preprocessed tmp file.")
    FileUtils.deleteQuietly(new File("data/output/tmp/"))
  }
}