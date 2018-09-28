package akkaCrawer.mainCrawer

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import java.net.URLEncoder

object CrawerTest {


  def main(args: Array[String]): Unit = {
      val URL = "https://search.51job.com/list/010000,000000,0000,00,9,99,java%25E5%25B7%25A5%25E7%25A8%258B,2,1.html?lang=c&stype=&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&providesalary=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare="
      var doc:Document =Jsoup.connect(URL).get()

      println(doc.select("div.el").select("p").select("span").select("a").attr("href"))

  }
}
