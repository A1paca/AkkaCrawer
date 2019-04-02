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
  * 此爬虫为猎聘网站下的爬虫设计
  */
object LiePinCrawer{
  //请求参数：dqs=地区，key=工作关键词，curPage=页数
  val URL ="https://www.liepin.com/zhaopin/?isAnalysis=&dqs=%s&pubTime=&salary=&subIndustry=&industryType=&compscale=&key=%s&init=-1&searchType=1&headckid=f7e0c6134efb914a&flushckid=1&compkind=&fromSearchBtn=2&sortFlag=15&ckid=48ad2f672866ef98&jobKind=&industries=&clean_condition=&siTag=k_cloHQj_hyIn0SLM9IfRg~fA9rXquZc5IkJpXC-Ycixw&d_sfrom=search_prime&d_ckId=4aa9bcfa173284d6457a24cb092a41f2&d_curPage=0&d_pageSize=40&d_headId=bf64f2e9294634842ebc9e26461793b9&curPage=%d"
  //工作地点
  val jobCitysMap = Map(
    "北京" -> "010",
    "上海" -> "020",
    "广州" -> "050020",
    "深圳" -> "050090")
  /**
    * @Description 用于解析Document，其map的数据格式为（工作名称，工作地点，公司名称，薪资，详情链接，发布时间）
    * @param doc 解析网页的Document
    * @param job 用于存储爬取的job信息
    * @return 返回爬取的条数
    */
  def parseLiePinDoc(doc: Document, job: ConcurrentHashMap[String, String]): Int= {
    var count = 0
    for (elem <- doc.select("div.sojob-item-main")) {
      job.put(elem.select("p.company-name").select("a").html + ":"
        +elem.select("div.job-info").select("h3").select("a").html ,
        elem.select("div.job-info").select("h3").select("a").html + ","
        + elem.select("div.job-info").select("span.area").html
        + elem.select("div.job-info").select("a.area").html+ ","
        + elem.select("p.company-name").select("a").html + ","
        + salarySplit(elem.select("div.job-info").select("span.text-warning").html )+ ","
        + elem.select("div.job-info").select("a").attr("href")+","+
        elem.select("p.time-info").select("time").attr("title")
      )
      count += 1
    }
    count
  }

  /**
    * 用于格式化爬取薪资的数据
    * @param liePinJob 爬取的猎聘薪资
    * @return 格式化后的薪资
    */
  def salarySplit(liePinJob: String ): String ={
    if(liePinJob != "面议" ){
      val jobSalarySplited: Array[String] = liePinJob.substring(0, liePinJob.length-1).split("-",2)
      val salaryLower = jobSalarySplited(0).toDouble / 12
      val salaryUpper = jobSalarySplited(1).toDouble / 12
      val jobSalary = salaryLower.formatted("%.1f") + "-" + salaryUpper.formatted("%.1f") + "万/月"
      jobSalary
    }else liePinJob
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
  def concurrentCrawler(url: String, jobTag: String, jobCityNumber:String, maxPage: Int, threadNum: Int, jobMap: ConcurrentHashMap[String, String]): ConcurrentHashMap[String, String] = {
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
      val jobCityNumber = jobCitysMap(jobCity)
      val threadNum = 1
      val t1 = System.currentTimeMillis
      val jobMap = concurrentCrawler(URL, jobTag, jobCityNumber, page, threadNum, new ConcurrentHashMap[String, String]())
      kafkaProducerUtils.kafkaUploadData(jobTag,jobMap)
      for (entry <- jobMap.entrySet) {
        println("上传数据：Key = " + entry.getKey + ", Value = " + entry.getValue)
      }
      val t2 = System.currentTimeMillis
      println(s"抓取数：$sum  重试数：$fail  耗时(秒)：" + (t2 - t1) / 1000)
    }else{
      println( jobCity + "地区不存在")
    }
  }
//测试
  def main(args: Array[String]): Unit = {
    startCrawler("java", "北京", 0)
  }
}
