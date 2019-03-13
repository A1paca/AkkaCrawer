package akkaCrawer.mainCrawer

import org.jsoup.Jsoup
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

object CrawerTest {
  def parseLiePinDoc(doc: Document, job: Map[String, String])= {
    var count = 0
    for (elem <- doc.select("div.el")) {
      job.put(count.toString, elem.select("p").select("span").select("a").attr("title")+","+
        elem.select("span.t3").html +","+
        elem.select("span.t2").select("a").attr("title")+","+
        elem.select("span.t4").html +","+
        elem.select("p").select("span").select("a").attr("href")+","+
        "\t"
      )
      count += 1
    }
  }

  def main(args: Array[String]): Unit = {
      //val url = "https://www.liepin.com/zhaopin/?isAnalysis=&dqs=%s&pubTime=&salary=&subIndustry=&industryType=&compscale=&key=%s&init=-1&searchType=1&headckid=f7e0c6134efb914a&flushckid=1&compkind=&fromSearchBtn=2&sortFlag=15&ckid=48ad2f672866ef98&jobKind=&industries=&clean_condition=&siTag=k_cloHQj_hyIn0SLM9IfRg~fA9rXquZc5IkJpXC-Ycixw&d_sfrom=search_prime&d_ckId=4aa9bcfa173284d6457a24cb092a41f2&d_curPage=0&d_pageSize=40&d_headId=bf64f2e9294634842ebc9e26461793b9&curPage=%d"
      //val url1=  url.format(URLEncoder.encode("010","UTF-8"),URLEncoder.encode("java","UTF-8"),1)
      //var doc:Document =Jsoup.connect(url1).get()
      //println(doc.select("div.job-info").select("h3").select("a").attr("href"))
      //println(doc.select("div.sojob-item-main").select("p.company-name").select("a").html)
    val url51 = "https://search.51job.com/list/000000,000000,0000,00,9,99,java,2,1.html?lang=c&stype=&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&providesalary=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare="
      var doc:Document =Jsoup.connect(url51).get()




  }
}
