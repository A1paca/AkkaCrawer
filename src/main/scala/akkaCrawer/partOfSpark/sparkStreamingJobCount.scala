package akkaCrawer.partOfSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.immutable
object sparkStreamingJobCount {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    var conf=new SparkConf().setMaster("spark://192.168.80.83:7077")
      .setAppName("SparkStreamKaflaWordCount Demo")
    var ssc=new StreamingContext(conf,Seconds(4))

    val zkQuorum= "spark82:2181,spark83:2181,spark84:2181"

    val groupId="spark_receiver"
    val topics=Map("java"->2)
    //通过3个receive
    val receiverDstream: immutable.IndexedSeq[ReceiverInputDStream[(String,String)]]=(1 to 3).map(x=>{
      val stream:ReceiverInputDStream[(String,String)]= KafkaUtils.createStream(ssc,zkQuorum,groupId,topics)
      stream
    })

    val unionDstream:DStream[(String,String)]=ssc.union(receiverDstream)

    val topicData:DStream[String]= unionDstream.map(_._2)

    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_,1))

    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    result.print()

    //开启计算
    ssc.start()
    ssc.awaitTermination()



  }
}

