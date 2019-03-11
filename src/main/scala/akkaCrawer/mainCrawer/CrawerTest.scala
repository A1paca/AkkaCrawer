package akkaCrawer.mainCrawer

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import java.net.URLEncoder

object CrawerTest {


  def main(args: Array[String]): Unit = {
      val URL = "https://www.liepin.com/zhaopin/?isAnalysis=&dqs=&pubTime=&salary=&subIndustry=&industryType=&compscale=&key=java&init=-1&searchType=1&headckid=f7e0c6134efb914a&flushckid=1&compkind=&fromSearchBtn=2&sortFlag=15&ckid=f7e0c6134efb914a&jobKind=&industries=&clean_condition=&siTag=k_cloHQj_hyIn0SLM9IfRg~-nQsjvAMdjst7vnBI-6VZQ&d_sfrom=search_prime&d_ckId=bf64f2e9294634842ebc9e26461793b9&d_curPage=0&d_pageSize=40&d_headId=bf64f2e9294634842ebc9e26461793b9"
      var doc:Document =Jsoup.connect(URL).get()

      //println(doc.select("div.job-info").select("h3").select("a").html)
      println(doc.select("div.sojob-item-main").select("p.company-name").select("a").html)
  }
}
