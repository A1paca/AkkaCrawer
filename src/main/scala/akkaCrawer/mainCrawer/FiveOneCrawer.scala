package akkaCrawer.mainCrawer

import java.io.{File, PrintWriter}
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
  * 此爬虫为51job网站下的爬虫设计
  */
object FiveOneCrawer{
  //请求参数，第一个“，”前的为工作地区，倒数第二个“，”前的为关键字，最后一个“，”后的为爬取页数
  val URL = "https://search.51job.com/list/%s00,000000,0000,00,9,99,%s,2,%d.html?lang=c&stype=&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&providesalary=99&lonlat=0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare="
  //工作地区Map
  val jobCitysMap = Map(
    "北京" -> "0100",
    "上海" -> "0200",
    "广州" -> "0302",
    "深圳" -> "0400")
  val jobCityToPinYin =Map(
    "北京" -> "Beijing",
    "上海" -> "Shanghai",
    "广州" -> "Guangzhou",
    "深圳" -> "Shenzhen"
  )
  /**
    * @Description 用于解析Document，其map的数据格式为（工作名称，工作地点，公司名称，薪资，详情链接，发布时间）
    * @param doc 解析网页的Document
    * @param job 用于存储爬取的job信息
    * @return 返回爬取的条数
    */
  def parseLiePinDoc(doc: Document, job: ConcurrentHashMap[String, String]): Int = {
    var count = 0
    for (elem <- doc.select("div.el")) {
     if(elem.select("p").select("span").select("a").attr("title").length != 0){
       job.put(elem.select("span.t2").select("a").attr("title") + ":"
         + elem.select("p").select("span").select("a").attr("title"),
         elem.select("p").select("span").select("a").attr("title")+","+
         elem.select("span.t3").html +","+
         elem.select("span.t2").select("a").attr("title")+","+
         elem.select("span.t4").html +","+
         elem.select("p").select("span").select("a").attr("href")+","+
         dateSplit(elem.select("span.t5").html)
       )
     }
      count += 1
    }
    count
  }

  /**
    * 用于格式化日期
    * @param fiveoneJob
    * @return 返回格式化后的日期
    */
  def dateSplit(fiveoneJob: String ): String ={
      val jobDateSplited: Array[String] = fiveoneJob.substring(0, fiveoneJob.length).split("-",2)
      val jobDate = "2019年"+jobDateSplited(0)+ "月" + jobDateSplited(1) + "日"
      jobDate
  }
  //用于记录总数，和失败次数
  val sum, fail: AtomicInteger = new AtomicInteger(0)
  /**
    * @Description 用于检测抓取的成功和失败
    * @param times 休眠时间
    * @param delay 等待时间
    * @param url 需要抓取的url
    * @param jobMap 存储爬取的job信息
    */
  def requestGetUrl(times: Int = 100, delay: Long = 10000)(url: String, jobMap: ConcurrentHashMap[String, String]): Unit = {
    Try(Jsoup.connect(url).get()) match {
      //使用try来判断是否成功和失败对网页进行抓取
      case Failure(e) =>
        if (times != 0) {
          println(e.getMessage)
          fail.addAndGet(1)
          Thread.sleep(delay)
          requestGetUrl(times - 1, delay)(url, jobMap)
        } else throw e
      case Success(doc) =>
        //成功抓取
        val count = parseLiePinDoc(doc, jobMap)
        if (count == 0) {
          Thread.sleep(delay)
          //递归进行再次抓取
          requestGetUrl(times - 1, delay)(url, jobMap)
        }
        sum.addAndGet(count);
    }
  }
  /**
    * @Description 用于设置并发编程
    * @param url 爬取的url
    * @param jobTag 工作关键字
    * @param jobCityNumber 用于替换url中的城市请求
    * @param maxPage 爬取的最大页数
    * @param threadNum 线程数
    * @param jobMap 用于存储爬取的数据
    */
  def concurrentCrawler(url: String, jobTag: String, jobCityNumber:String, maxPage: Int, threadNum: Int, jobMap: ConcurrentHashMap[String, String]): ConcurrentHashMap[String, String]  = {
    val loopPar = (0 to maxPage).par
    // 设置并发线程数
    loopPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(threadNum))
    // 利用并发集合多线程同步抓取:遍历所有页
    loopPar.foreach(i => requestGetUrl()(url.format(URLEncoder.encode(jobCityNumber, "UTF-8"), URLEncoder.encode(jobTag, "UTF-8"), 20 * i), jobMap))
    //输出格式
    jobMap
  }

  //直接输出
  def saveFile(file: String, jobMap: ConcurrentHashMap[String, String]): Unit = {
    val writer = new PrintWriter(new File(new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + file ++ ".txt"))
    for ((_, value) <- jobMap) writer.println(value)
    writer.close()
  }
  /**
    * @Description 用于开始爬取函数
    * @param jobTag 设置的爬取工作关键词
    * @param jobCity 需要爬取的工作城市
    * @param page 页数
    */
  def startCrawler( jobTag: String, jobCity:String ,page :Int): Unit = {
    if( jobCitysMap.contains( jobCity)){
      //取出爬取的城市所在url中的表示方式
      val jobCityNumber = jobCitysMap(jobCity)
      val threadNum = 1
      val t1 = System.currentTimeMillis
      val jobMap = concurrentCrawler(URL, jobTag, jobCityNumber, page, threadNum, new ConcurrentHashMap[String, String]())
      kafkaProducerUtils.kafkaUploadData(jobCityToPinYin(jobCity),jobMap)
      val t2 = System.currentTimeMillis
      println(s"抓取数：$sum  重试数：$fail  耗时(秒)：" + (t2 - t1) / 1000)
    }else{
      println( jobCity + "地区不存在")
    }
  }
  //测试
  def main(args: Array[String]): Unit = {
    startCrawler("java", "广州", 1)
  }
}
