# AkkaCrawer
## 相关版本

### 主要框架版本

| 名称  |  版本  |
| :---: | :----: |
| Akka  | 2.5.3  |
| Spark | 2.1.0  |
| Kafka | 0.11.0 |

### 所用第三方包以及语言版本

|              名称               |  版本  |
| :-----------------------------: | :----: |
|              Scala              |  2.11  |
|              Jsoup              | 1.11.3 |
| Spark-Streaming-Kafka-0-10_2.11 | 2.1.0  |

## 快速开始

### 运行前准备

1.启动Zookeeper

```
zkServer.sh start
```

2.启动Kafka

```
~/YourKafkaPath/bin/kafka-server-start.sh ~/YourKafkaPath/config/server.properties &
```