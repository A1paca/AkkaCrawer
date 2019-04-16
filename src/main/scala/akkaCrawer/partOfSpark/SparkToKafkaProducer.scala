package akkaCrawer.partOfSpark

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.BasicConfigurator

object SparkToKafkaProducer {
  def resultUpload( Beijing: String, Shenzhen: String, Shanghai:String, Guangzhou:String): Unit ={
    BasicConfigurator.configure()
    def BROKER_LIST = "192.168.80.82:9092,192.168.80.83:9092,192.168.80.84:9092"
    def TOPIC = "result"
    println("开始产生消息")
    val props = new Properties()
    //配置Producer的配置
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    producer.send(new ProducerRecord(TOPIC,null,
      "(Beijing" +":" +Beijing +")"+",(Shenzhen" +":" +Shenzhen +")"+",(Shanghai" +":" +Shanghai +")"+",(Guangzhou" +":" +Guangzhou +")" ))
    println("平均值结果生产完毕")

    producer.close
  }
}
