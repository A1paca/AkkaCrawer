package akkaCrawer.deploy

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._


class Worker(var workerHost: String, var workerPort: String) extends Actor {
  var masterActor: ActorSelection = null
  val workerId = UUID.randomUUID().toString

  val HearBeatIntever = 3000


  //worker启动后会立即执行
  override def preStart(): Unit = {
    masterActor = context.actorSelection("akka.tcp://masterActorSystem@localhost:9980/user/masterActor")
    //worker启动后立即向master进行注册
    masterActor ! RegisterWorker(workerId, workerHost, workerPort)
  }

  //接收消息的方法
  override def receive: Receive = {
    case "started" => println("[提示]worker启动成功")
    //接收master发送过来的注册成功的消息
    case RegisteredWorker => {
      println("[汇报]收到注册成功的消息，开始发送心跳")
      //导入定时器
      import context.dispatcher
      //开启定时任务
      context.system.scheduler.schedule(0 millis, HearBeatIntever millis, self, SendHearBeat)

    }
    //向master汇报心跳
    case SendHearBeat => {
      //println("[汇报]正在向master发送心跳")
      masterActor ! HearBeat(workerId)
    }
    //开始爬虫工作
    case StartJob(job, page) =>{

      println("[开始爬虫工作]关键词："+job+"页数："+page)

      println("[爬虫已结束]")
      masterActor ! JobFinish(workerId,workerHost,workerPort)
    }

  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    val workerHost = "localhost"
    val workerPort = "9983"

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$workerHost"
         |akka.remote.netty.tcp.port = "$workerPort"
      """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem
    val workerActorSystem = ActorSystem("workerActorSystem", config)
    //使用ActorSystem来创建Actor
    val workerActor = workerActorSystem.actorOf(Props(new Worker(workerHost, workerPort)), "workerActor")
    workerActor ! "started"
    //将ActorSystem 阻塞在这，不要让其停止
    workerActorSystem.whenTerminated
  }

}
