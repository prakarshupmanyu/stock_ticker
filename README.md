# About this project
This is a work in progress. Currently, the code works on your local laptop. It is aimed to work as docker app which will have separate containers for each of the components used in this project.

## Prerequisities

You should have Maven, Git, Java 8, Scala (2.12), Spark (3.1.1) and Kafka (2.5.0) installed.

## Steps

1 - Clone the repo
``` shell
git clone https://github.com/prakarshupmanyu/stock_ticker.git
```
2 - Go to the base directory of the project. Once there, build the JAR
``` shell
mvn clean compile assembly:single
```
3 - The uber JAR would be created and stored under target/ directory
```shell
ls target/
```
4 - From you kafka base directory, run the zookeeper and the kafka broker
```shell
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties
```
5 - After the Kafka broker is running, let's launch the producer. From your base project directory, run this
```shell
scala -classpath "target/stock_ticker-1.0-SNAPSHOT-jar-with-dependencies.jar" producer.StockProducer
```
6 - You will see the messages being sent to the Kafka topic. To run the consumer, run this command
```shell
spark-submit --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --class consumer.StockConsumer target/stock_ticker-1.0-SNAPSHOT-jar-with-dependencies.jar
```
I have made the consumer work with Spark, just for fun. Running this command might give you error because of version incompatibilities and dependencies. I am working on fixing them once I move all of this to docker.

Another way to run test the consumer code is open spark-shell and basically type in the consumer code
```shell
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1

scala> :paste

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{avg, col, from_json}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.DataFrame

val BROKER_LIST = "localhost:9092"
val TOPIC = "stocks"

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

```

Press Ctrl + D after that and you'll see the output.
