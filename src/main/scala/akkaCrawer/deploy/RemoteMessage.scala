package akkaCrawer.deploy

import scala.collection.mutable

trait RemoteMessage extends Serializable {}

//注册worker
case class RegisterWorker(var workerId: String, var workerHost: String, var workerPort: String) extends RemoteMessage

//成功注册worker
case class RegisteredWorker() extends RemoteMessage



//发送心跳
object SendHearBeat

//心跳
case class HearBeat(var workerId: String)

//检测是否超时
object CheckTimeOutWorker



//client请求开始爬虫
case class ClientBegin()

//master同意client开始爬虫
case class AcceptClientBegin()

//client发送要爬取的关键词和页数
case class ClientToMaster(var job: mutable.HashMap[Int,String])extends RemoteMessage

//开始爬虫的操作
case class StartJob(var job: String,var page: Int) extends RemoteMessage
//爬虫结束
case class JobFinish(var workerId: String,var workerHost: String,var workerPort: String) extends RemoteMessage


//worker繁忙
case class BusyWorker(var job:String,var page: Int)
