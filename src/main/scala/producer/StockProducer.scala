package producer

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

case class Stock(name: String, price: Int)
case class Message(tickers: Array[Stock])

//command line => scala -classpath "target/stock_ticker-1.0-SNAPSHOT-jar-with-dependencies.jar" producer.StockProducer

object StockProducer extends App {

  val BROKER_LIST = "localhost:9092"
  val TOPIC = "stocks"
  val BATCH_SIZE = 10
  val TIME_TO_WAIT_MILLIS: Long = 100

  val random = new Random()
  val gson = new Gson

  val AMZN_MEAN_PRICE = 1902
  val AMZN_MIN_PRICE = getStockMinPrice(AMZN_MEAN_PRICE)
  val AMZN_MAX_PRICE = getStockMaxPrice(AMZN_MEAN_PRICE)

  val MSFT_MEAN_PRICE = 107
  val MSFT_MIN_PRICE = getStockMinPrice(MSFT_MEAN_PRICE)
  val MSFT_MAX_PRICE = getStockMaxPrice(MSFT_MEAN_PRICE)

  val AAPL_MEAN_PRICE = 215
  val AAPL_MIN_PRICE = getStockMinPrice(AAPL_MEAN_PRICE)
  val AAPL_MAX_PRICE = getStockMaxPrice(AAPL_MEAN_PRICE)

  val stockMap: Map[Int, String] = Map(1 -> "AMZN", 2 -> "MSFT", 3 -> "AAPL")
  val priceMap: Map[Int, (Int, Int)] = Map(1 -> (AMZN_MIN_PRICE, AMZN_MAX_PRICE), 2 -> (MSFT_MIN_PRICE, MSFT_MAX_PRICE),
    3 -> (AAPL_MIN_PRICE, AAPL_MAX_PRICE))

  val properties = new Properties()
  properties.put("bootstrap.servers", BROKER_LIST)
  //properties.put("batch.size", BATCH_SIZE)
  //properties.put("linger.ms", TIME_TO_WAIT_MILLIS)
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)

  def getStockMinPrice(meanPrice: Int): Int = {
    (0.9 * meanPrice).toInt
  }

  def getStockMaxPrice(meanPrice: Int): Int = {
    (1.1 * meanPrice).toInt
  }

  def generateStockMessage(): String = {
    val numTickers = 1 + random.nextInt(3) // 1, 2 or 3
    val stockKeysVisited = scala.collection.mutable.Set[Int]()
    val stocks = new Array[Stock](numTickers)
    for (i <- 1 to numTickers) {
      var randomStockKey = 1 + random.nextInt(3)
      while (stockKeysVisited.contains(randomStockKey)) {
        randomStockKey = 1 + random.nextInt(3)
      }
      stockKeysVisited.add(randomStockKey)
      val stockName: String = stockMap.get(randomStockKey) match {
        case None => ""
        case Some(s) => s
      }
      val stockPriceRange: Int = priceMap.get(randomStockKey).toSeq(0)._2 - priceMap.get(randomStockKey).toSeq(0)._1
      val randomStockPrice: Int = priceMap.get(randomStockKey).toSeq(0)._1 + random.nextInt(stockPriceRange + 1)
      stocks(i - 1) = Stock(stockName, randomStockPrice)
    }
    gson.toJson(Message(stocks))
  }

  def sendBatch(): Unit = {
    var keyCounter = 1
    while (true) {
      for (i <- 1 to 10) {
        val msg = new ProducerRecord[String, String](TOPIC, keyCounter.toString, generateStockMessage())
        println("Sending message :: " + msg)
        producer.send(msg)
        keyCounter += 1
      }
      Thread.sleep(1000)
    }
  }

  sendBatch()
}
