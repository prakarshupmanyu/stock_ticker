package consumer
/*
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
*/
import org.apache.spark.SparkConf

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{avg, col, from_json}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.DataFrame

import scala.util.Properties

object StockConsumer extends App {

  val BROKER_LIST = "localhost:9092"
  val TOPIC = "stocks"

  val conf = new SparkConf

  //conf.set("spark.master", Properties.envOrElse("SPARK_MASTER_URL", "spark://spark-master:7077"))
  //conf.set("spark.driver.host", Properties.envOrElse("SPARK_DRIVER_HOST", "local[*]"))
  conf.set("spark.submit.deployMode", "client")
  //conf.set("spark.driver.bindAddress", "0.0.0.0")
  conf.set("partition.assignment.strategy", "")
  conf.set("spark.app.name", "StockConsumer")

  val spark = SparkSession
    .builder()
    .config(conf = conf)
    .getOrCreate()

  val df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", BROKER_LIST)
    .option("partition.assignment.strategy", "range")
    .option("startingOffsets", "earliest")
    .option("subscribe", TOPIC)
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
          val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }

  val schema = new StructType(Array(StructField("tickers", ArrayType(StructType(Array(StructField("name", StringType), StructField("price", IntegerType)))))))
  val df2 = df.withColumn("valueData", from_json(col("value"), schema)).select("valueData.*")

  val finalDf = flattenDataframe(df2).groupBy("tickers_name").agg(avg("tickers_price").as("avg_price"))

  while(true) {
    finalDf.show
    println("Time :: " + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now) + "\n\n\n")
    Thread.sleep(30000)
  }
}

/*
* val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("startingOffsets", "earliest")
  .option("subscribe", "test")
  .load()

  val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
  import org.apache.spark.sql.types.StringType
  import org.apache.spark.sql.types.IntegerType
  import org.apache.spark.sql.types.StructField
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.types.ArrayType

  val schema = new StructType(Array(StructField("tickers", ArrayType(StructType(Array(StructField("name", StringType), StructField("price", IntegerType)))))))
  val df3 = df2.withColumn("valueData", from_json(col("value"), schema)).select("valueData.*")

  val df4 = df3.withColumn("stock1", col("tickers").getItem(0)).withColumn("stock2", col("tickers").getItem(1)).withColumn("stock3", col("tickers").getItem(2))
  val df5 = df4.select("stock1", "stock2", "stock3")
  df5.writeStream.format("console").start()
*
*
* /*
  val properties = new Properties()
  properties.put("bootstrap.servers", BROKER_LIST)
  properties.put("group.id", "stock-consumers")
  properties.put("partition.assignment.strategy", "")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  properties.put("key.deserializer", classOf[StringDeserializer])
  properties.put("value.deserializer", classOf[StringDeserializer])

  val kafkaConsumer = new KafkaConsumer[String, String](properties)
  kafkaConsumer.subscribe(TOPIC)

  while (true) {
    val results = kafkaConsumer.poll(2000).asScala
    println(results)
    for ((topic, data) <- results) {
      println(topic + " :: " + data)
    }
  }
  */
* */