package akkaCrawer.mainCrawer

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object kafkaProducerUtils {
  /**
    * 此方法用于上传数据
    * @param topic:topic名称
    * @param brokerList：broker地址
    */
  def kafkaUploadData(brokerList:String, topic:String, jobMap: ConcurrentHashMap[String, String]): Unit ={
    def BROKER_LIST = topic
    def TOPIC = brokerList
    println("开始产生消息！")
    val props = new Properties()
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    for (entry <- jobMap.entrySet) {
      //producer.send(new ProducerRecord(TOPIC, entry.getKey, entry.getValue))
      println("Key = " + entry.getKey + ", Value = " + entry.getValue)
    }
    producer.close
  }
}