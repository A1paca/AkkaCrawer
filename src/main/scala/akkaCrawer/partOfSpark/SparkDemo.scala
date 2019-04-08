package akkaCrawer.partOfSpark



import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test01")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.streaming.backpressure.enabled", "false")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.kafka.maxRetries", "3")

    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(1))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "192.168.80.82:9092,192.168.80.83:9092,192.168.80.84:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test001",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    /** 可以配置多个 */
    val topics = Array("java")

    /** 2、这种订阅会读取所有的partition数据 */
    val stream2: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream2.foreachRDD(lineRDD => {
      if (!lineRDD.isEmpty()) {
        lineRDD.foreachPartition(iter => {
          iter.foreach(record => {
            println("partition = " + record.partition() ," key = " + record.key(), " value = " + record.value(), " offset = " + record.offset())
          })
        })

      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}