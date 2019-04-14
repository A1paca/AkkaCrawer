package akkaCrawer.partOfSpark
import java.sql.{Connection, DriverManager}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
object DBUtil {
  def insertDataBase(jobTag:String,jobMap: ConcurrentHashMap[String, String]) = {
    // 访问本地MySQL服务器，通过3306端口访问mysql数据库
    val url = "jdbc:mysql://localhost:3306/crawerdata?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    //驱动名称
    val driver = "com.mysql.jdbc.Driver"
    //用户名
    val username = "root"
    //密码
    val password = "password"
    //初始化数据连接
    var connection: Connection = null
    try {

      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement

      //创建jobtag为名字的表
      var creatStr =s"CREATE TABLE $jobTag(" +
        s"jobnum VARCHAR(20) PRIMARY KEY NOT NULL," +
        s"jobname VARCHAR(200), " +
        s"jobsalary VARCHAR(200), " +
        s"jobarea VARCHAR(200))charset=utf8;"
      var rs1 = statement.executeLargeUpdate(creatStr)

      //向数据库中插入信息
      for (e <- jobMap.entrySet) {
        var jobNum = e.getKey
        var jobInfo = e.getValue.split(",")
        var rs2 = statement.executeUpdate(
          s"INSERT INTO `$jobTag` (`jobnum`, `jobname`,`jobsalary`,`jobarea`) VALUES ('$jobNum', '${jobInfo(0)}', '${jobInfo(1)}', '${jobInfo(2)}')")
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    //关闭连接，释放资源
    connection.close

  }
}
