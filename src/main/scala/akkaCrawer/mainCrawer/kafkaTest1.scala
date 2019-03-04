package akkaCrawer.mainCrawer
import java.util.Properties


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object kafkaTest1{
  def BROKER_LIST = "spark82:9092"
  def TOPIC = "kafka_test_3"

  def main(args: Array[String]): Unit = {
    println("开始产生消息！")
    val props = new Properties()
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    while(true){
      for (i <- 0 to 10) {
        producer.send(new ProducerRecord(TOPIC, "key-" + i, "msg-" + i))
        println(i)
      }
      Thread.sleep(3000)
    }
    producer.close
  }

}

