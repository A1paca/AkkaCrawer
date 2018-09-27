package akkaCrawer.deploy

class WorkerInfo(var workerId: String, var workerHost: String, var workerPort: String) {

  //worker最近汇报心跳的时间
  var lastHearBeatTime: Long = System.currentTimeMillis()
}
class ClientInfo(var clientId: String,var clientHost: String,var clientPort: String){
  //client最近汇报心跳时间
  var lastHearBeatTime: Long = System.currentTimeMillis()
}
