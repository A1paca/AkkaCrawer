package akkaCrawer.deployAkka

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

class Master extends Actor {

  //封装workerInfo
  val idToWorker = new mutable.HashMap[String, WorkerInfo]()
  //WorkerInfo
  val workers = new mutable.HashSet[WorkerInfo]()
  //空闲worker
  val freeWorker = new mutable.HashMap[String, String]()
  //繁忙worker
  val busyWorker = new mutable.HashMap[String, String]()
  //检测时间
  val CheckTimeOutWorkerIntever = 5000


  //preStart方法是在actor启动后会立马执行的方法
  override def preStart(): Unit = {
    import context.dispatcher
    //启动一个定时器定时检查超时的worker
    context.system.scheduler.schedule(0 millis, CheckTimeOutWorkerIntever millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {
    case "started" => println("[提示]master启动成功")

    //接收worker发送过来的注册消息
    case RegisterWorker(workerId, workerHost, workerPort) => {
      if (!idToWorker.contains(workerId)) {
        //将发送过来的worker信息进行封装成workerInfo保存到map和set中
        val workerInfo = new WorkerInfo(workerId, workerHost, workerPort)
        idToWorker.put(workerId, workerInfo)
        freeWorker.put(workerPort,workerHost)
        workers += workerInfo
        println("[提示]"+"WORKER:"+workerId+" "+workerHost+" "+workerPort+"正在注册")
        //发送注册成功的消息
        sender() ! RegisteredWorker
      }else println("注册失败:workerID重复")
    }
    //接收爬虫工作已经结束的消息
    case JobFinish(workerId,workerHost,workerPort) => {
      println("[工作]"+workerId+"爬虫工作已结束")
      //从繁忙map中删除
      busyWorker.remove(workerPort)
      //添加到空闲map中
      freeWorker.put(workerPort,workerHost)
    }

    //接收worker汇报心跳
    case HearBeat(workerId) => {
      //从idToWorker中取出对应的worker更新最近一次汇报心跳的时间
      if (idToWorker.contains(workerId)) {
        //println("接收到了"+workerId+"的心跳")
        val workerInfo = idToWorker(workerId)
        workerInfo.lastHearBeatTime = System.currentTimeMillis()
      }
    }
    //检测超时的worker
    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      //过滤出超时的worker
      val toRemove = workers.filter(w => currentTime - w.lastHearBeatTime > CheckTimeOutWorkerIntever)
      toRemove.foreach(workerInfo => {
        idToWorker.remove(workerInfo.workerId)
        workers -= workerInfo
      })
      println(s"[汇报]存活了${workers.size}个worker,")
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val masterHost = "localhost"
    val masterPort = "9980"

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$masterHost"
         |akka.remote.netty.tcp.port = "$masterPort"
      """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem
    val masterActorSystem = ActorSystem("masterActorSystem", config)
    //使用ActorSystem来创建Actor
    val masterActor = masterActorSystem.actorOf(Props[Master], "masterActor")
    //给新创建的masterActor发送一条消息,发送消息用 感叹号 “ ！” 进行发送
    masterActor ! "started"
    //将ActorSystem 阻塞
    masterActorSystem.whenTerminated

  }
}
