
# 1. SparkUI界面介绍

可以指定提交Application的名称

```shell script
./spark-shell --master  spark://node1:7077 --name myapp
```

# 2. 配置historyServer

## 2.1 临时配置

对本次提交的应用程序起作用

```shell script
./spark-shell --master spark://node1:7077 
--name myapp1
--conf spark.eventLog.enabled=true
 --conf spark.eventLog.dir=hdfs://node1:9000/spark/test
```

停止程序，在Web Ui中Completed Applications对应的ApplicationID中能查看history。

## 2.2 spark-default.conf配置

spark-default.conf配置文件中配置HistoryServer，对所有提交的Application都起作用

在客户端节点，进入`$SPARK_HOME/conf/`，在`spark-defaults.conf`最后加入:
```shell script
#开启记录事件日志的功能
spark.eventLog.enabled          true
#设置事件日志存储的目录
spark.eventLog.dir              hdfs://node1:9000/spark/test
#设置HistoryServer加载事件日志的位置
spark.history.fs.logDirectory   hdfs://node1:9000/spark/test
#日志优化选项,压缩日志
spark.eventLog.compress         true
```

启动HistoryServer：
```shell script
./start-history-server.sh
```
访问HistoryServer：node4:18080,之后所有提交的应用程序运行状况都会被记录。