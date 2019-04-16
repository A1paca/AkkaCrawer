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


import scala.collection.mutable.ListBuffer


object SparkDemo {
  //用于存储薪资的数据
  val jobSalaryValue :ListBuffer[ListBuffer[String]] = ListBuffer(
    ListBuffer(),
    ListBuffer(),
    ListBuffer(),
    ListBuffer()
  )
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test01")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.streaming.backpressure.enabled", "false")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.kafka.maxRetries", "3")

    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "192.168.80.82:9092,192.168.80.83:9092,192.168.80.84:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test001",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    /**配置多个 */
    val topics = Array("Beijing","Shenzhen","Shanghai","Guangzhou")

    /** 读取所有的partition数据 */
    val stream2: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream2.foreachRDD(lineRDD => {
      println("-----正在获取RDD信息-----")
      if (!lineRDD.isEmpty()) {
        lineRDD.foreachPartition(iter => {
          println("-----正在获取Partition信息-----")
          iter.foreach(record => {
            //println("partition = " + record.partition() ," key = " + record.key(), " value = " + record.value(), " offset = " + record.offset())
            val jobInfoSplited: Array[String] = record.value().split(";")
            val jobName = jobInfoSplited(0)
            val jobLocation = jobInfoSplited(1)
            val companyName = jobInfoSplited(2)
            val jobSalary = jobInfoSplited(3)
            val jobLink = jobInfoSplited(4)
            val jobDate = jobInfoSplited(5)
            //println("工作名称：" + jobName +"工作地点："+ jobLocation + "公司名称："  + companyName + "薪资："+ jobSalary + "详情链接："+ jobLink +"日期："+ jobDate)
            //求出上下限的中间值，并存入jobSalaryValue中
            jobMedCount(jobSalary,record)
          })
        })
        SparkToKafkaProducer.resultUpload(
          salaryAverage(jobSalaryValue(0)),
          salaryAverage(jobSalaryValue(1)),
          salaryAverage(jobSalaryValue(2)),
          salaryAverage(jobSalaryValue(3)))
        //println(jobSalaryValue)
      }else {
        println("-----初始化数据集-----")
        jobSalaryValue(0).remove(0,jobSalaryValue(0).length)
        jobSalaryValue(1).remove(0,jobSalaryValue(1).length)
        jobSalaryValue(2).remove(0,jobSalaryValue(2).length)
        jobSalaryValue(3).remove(0,jobSalaryValue(3).length)
        println("北京地区数据是否为空："+jobSalaryValue(0).isEmpty)
        println("深圳地区数据是否为空："+jobSalaryValue(1).isEmpty)
        println("上海地区数据是否为空："+jobSalaryValue(2).isEmpty)
        println("广州地区数据是否为空："+jobSalaryValue(3).isEmpty)
        println("-----初始化数据集完毕-----")
      }
      println("-----RDD信息获取完毕-----")
    })



    def jobMedCount(jobSalary :String ,record :ConsumerRecord[String,String]) = {
      try {
        if (jobSalary != "面议" & !jobSalary.isEmpty) {
          //从薪资中提取数字信息（使用分割的方法）
          val jobSalarySplit: Array[String] = jobSalary.substring(0, jobSalary.length - 3).split("-", 2)
          //取薪资中间值
          val jobMed = ((jobSalarySplit(1).toDouble - jobSalarySplit(0).toDouble) / 2) + jobSalarySplit(0).toDouble
          //从topic判断地区，加入到相应的ListBuffer中
          record.topic() match {
            case "Beijing" => jobSalaryValue(0) += jobMed.formatted("%.1f").toString
            case "Shenzhen"=> jobSalaryValue(1) += jobMed.formatted("%.1f").toString
            case "Shanghai" => jobSalaryValue(2) += jobMed.formatted("%.1f").toString
            case "Guangzhou"=> jobSalaryValue(3) += jobMed.formatted("%.1f").toString
          }
        }
      } catch {
        case error: java.lang.ArrayIndexOutOfBoundsException =>
          println("数据不完整，该条数据已抛弃")
      }
    }

    def salaryAverage(value:ListBuffer[String])={
      var sum:Double = 0
      for (s <- value){
        sum += s.toDouble
      }
      (sum/value.length).formatted("%.1f")
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}