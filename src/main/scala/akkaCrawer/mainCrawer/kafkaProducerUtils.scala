package akkaCrawer.mainCrawer
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.BasicConfigurator

object kafkaProducerUtils {
  /**
    * 此方法用于将Akka作为生产者上传数据
    * @param topic:topic名称
    */
  def kafkaUploadData( topic:String, jobMap: ConcurrentHashMap[String, String]): Unit ={
    BasicConfigurator.configure()
    def BROKER_LIST = "192.168.80.82:9092,192.168.80.83:9092,192.168.80.84:9092"
    def TOPIC = topic
    println("开始产生消息")
    val props = new Properties()
    //配置Producer的配置
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)
    //将jobMap中的数据全部上传
    for (entry <- jobMap.entrySet) {
      producer.send(new ProducerRecord(TOPIC, entry.getKey, entry.getValue))
      println("上传数据：Key = " + entry.getKey + ", Value = " + entry.getValue)
    }
    producer.close
    println("数据生产完毕")
  }
}
