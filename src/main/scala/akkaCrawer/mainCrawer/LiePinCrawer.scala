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



object LiePinCrawer{
  val URL ="https://www.liepin.com/zhaopin/?isAnalysis=&dqs=&pubTime=&salary=&subIndustry=&industryType=&compscale=&key=%s&init=-1&searchType=1&headckid=f7e0c6134efb914a&flushckid=1&compkind=&fromSearchBtn=2&sortFlag=15&ckid=48ad2f672866ef98&jobKind=&industries=&clean_condition=&siTag=k_cloHQj_hyIn0SLM9IfRg~fA9rXquZc5IkJpXC-Ycixw&d_sfrom=search_prime&d_ckId=4aa9bcfa173284d6457a24cb092a41f2&d_curPage=0&d_pageSize=40&d_headId=bf64f2e9294634842ebc9e26461793b9&curPage=%d"
  //解析Document，需要对照网页源码进行解析
  //数据格式=（工作名称，工作地点，公司名称，薪资，详情链接）
  def parseLiePinDoc(doc: Document, job: ConcurrentHashMap[String, String]): Int= {
    var count = 0
    for (elem <- doc.select("div.sojob-item-main")) {
      job.put(count.toString, elem.select("div.job-info").select("h3").select("a").html + ","
        + elem.select("div.job-info").select("span.area").html + ","
        + elem.select("p.company-name").select("a").html + ","
        + elem.select("div.job-info").select("span.text-warning").html + ","
        + elem.select("div.job-info").select("a").attr("href")
        +"\t"
      )
      count += 1
    }
    count
  }

  //用于记录总数，和失败次数
  val sum, fail: AtomicInteger = new AtomicInteger(0)
  //抓取检测成功失败
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
  //设置并发编程
  def concurrentCrawler(url: String, jobTag: String, maxPage: Int, threadNum: Int, jobMap: ConcurrentHashMap[String, String]): Unit = {
    val loopPar = (0 to maxPage).par
    // 设置并发线程数
    loopPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(threadNum))
    // 利用并发集合多线程同步抓取:遍历所有页
    loopPar.foreach(i => requestGetUrl()(url.format(URLEncoder.encode(jobTag, "UTF-8"), 20 * i), jobMap))
    //输出格式
    for (entry <- jobMap.entrySet) {

      println("上传数据：Key = " + entry.getKey + ", Value = " + entry.getValue)
    }

  }

  //直接输出
  def saveFile(file: String, jobMap: ConcurrentHashMap[String, String]): Unit = {
    val writer = new PrintWriter(new File(new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + file ++ ".txt"))
    for ((_, value) <- jobMap) writer.println(value)
    writer.close()
  }
  //开始爬虫函数
  def startCrawler( jobTag: String,page :Int): Unit = {
    //线程数
    val threadNum = 3
    val t1 = System.currentTimeMillis
    concurrentCrawler(URL, jobTag, page, threadNum, new ConcurrentHashMap[String, String]())
    val t2 = System.currentTimeMillis
    println(s"抓取数：$sum  重试数：$fail  耗时(秒)：" + (t2 - t1) / 1000)
  }
//测试
  def main(args: Array[String]): Unit = {
    startCrawler("java",0)
  }
}
