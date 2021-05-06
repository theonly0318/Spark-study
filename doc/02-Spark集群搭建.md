Spark 集群搭建


# 1. 节点
| node1    | node2             | node3      | node4      |
| -------- | ----------------- | ---------- | ---------- |
| NameNode-1 | NameNode-2|            |            |
|          | DataNode-1        | DataNode-2 | DataNode-3 |
|          | Zookeeper-1        | Zookeeper-2 | Zookeeper-3 |
| zkfc-1 | zkfc-2|            |            |
|          | JournalNode-1        | JournalNode-2 | JournalNode-3 |
|			|								|	ResourceManager-1	|	ResourceManager-2	|
|			| NodeManager-1 |	NodeManager-2	|	NodeManager-3	|
| Spark MASTER | Spark Worker | Spark Worker | Spark Client |

# 2. 下载安装包

[下载地址](http://spark.apache.org/downloads.html)

上传至集群某节点，解压

```shell script
tar -zxvf spark-2.3.1-bin-hadoop2.6.tgz
mv spark-2.3.1-bin-hadoop2.6 /opt/spark-2.3.1
```

# 3. 配置

- 配置`slaves`

    ```shell script
    cd /opt/spark-2.3.1/conf
    cp slaves.template slaves
    vim slaves
    ```
    在slaves中配置worker节点
    ```
    node2
    node3
    ```

- 配置`spark-env.sh`
    ```shell script
    cd /opt/spark-2.3.1/conf
    cp spark-env.sh.template spark-env.sh
    vim spark-env.sh
    ```
    修改：
    - SPARK_MASTER_HOST:master的ip
    - SPARK_MASTER_PORT:提交任务的端口，默认是7077
    - SPARK_WORKER_CORES：每个worker从节点能够支配的core的个数
    - SPARK_WORKER_MEMORY:每个worker从节点能够支配的内存数
    - JAVA_HOME:java的home，这里需要jdk8
    
    ```shell script
    export SPARK_MASTER_HOST=node1
    export SPARK_MASTER_PORT=7077
    export SPARK_WORKER_CORES=2
    export SPARK_WORKER_MEMORY=3g
    export JAVA_HOME=/usr/java/jdk1.8.0_221-amd64
    ```
  
  
# 4. 同步至其他节点
```shell script
cd /opt
scp -r ./spark-2.3.1/ node2:`pwd` &&  scp -r ./spark-2.3.1/ node3:`pwd`
```

# 5. 启动集群
```shell script
cd /opt/spark-2.3.1/sbin
./start-all.sh
```

# 6. 搭建客户端

将spark安装包原封不动的拷贝到一个新的节点上，然后，在新的节点上提交任务即可。

注意：

8080是Spark WEBUI界面的端口，7077是Spark任务提交的端口。

修改master的WEBUI端口：
    
1. 修改$SPARK_HOME/conf/spark-env.sh【建议使用】：
        
```shell script
export SPARK_MASTER_WEBUI_PORT=9999
```
        
2. 修改start-master.sh

3. 在Master节点上导入临时环境变量，只是作用于之后的程序，重启就无效了。




# Hadoop YARN
Spark 也可以基于Yarn进行任务调度，这就是所谓的Spark on Yarn，Spark基于Yarn进行任务调度只需要在Spark客户端做如下配置即可：
```shell script
cd /opt/spark-2.3.1/conf
vim spark-env.sh
```
```shell script
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

同时这里需要在每台NodeManager节点中将每台NodeManager的虚拟内存关闭，在每台NodeManager节点的`$HADOOP_HOME/etc/hadoop/yarn-site.xml`中加入如下配置：
```xml
<!-- 关闭虚拟内存检查 -->
<property>
	<name>yarn.nodemanager.vmem-check-enabled</name>
	<value>false</value>
</property>
```

# Spark Pi任务测试

在客户端提交

Standalone提交命令：
```shell script
./spark-submit --master spark://node1:7077 --class org.apache.spark.examples.SparkPi ../examples/jars/spark-examples_2.11-2.3.1.jar 100
```

Spark on YARN提交命令：
```shell script
./spark-submit --master yarn --class org.apache.spark.examples.SparkPi ../examples/jars/spark-examples_2.11-2.3.1.jar 100
```


# Master HA

## Master的高可用原理
Standalone集群只有一个Master，如果Master挂了就无法提交应用程序，需要给Master进行高可用配置，Master的高可用可以使用fileSystem(文件系统)和zookeeper（分布式协调服务）。

fileSystem只有存储功能，可以存储Master的元数据信息，用fileSystem搭建的Master高可用，在Master失败时，需要我们手动启动另外的备用Master，这种方式不推荐使用。

zookeeper有选举和存储功能，可以存储Master的元素据信息，使用zookeeper搭建的Master高可用，当Master挂掉时，备用的Master会自动切换，推荐使用这种方式搭建Master的HA。

## Master高可用搭建
1. 在Spark Master节点上配置主Master，配置spark-env.sh
```shell script
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=node2:2181,node3:2181,node4:2181 -Dspark.deploy.zookeeper.dir=/sparkmaster0821"
```
2. 发送到其他worker节点上
```shell script
scp spark-env.sh node2:`pwd` && scp spark-env.sh node3:`pwd`
```

3. 找一台节点（非主Master节点）配置备用 Master,修改spark-env.sh配置节点上的MasterIP
```shell script
export SPARK_MASTER_IP=node2
```
4. 启动集群之前启动zookeeper集群和Hadoop集群

5. 启动spark Standalone集群，启动备用Master

6. 打开主Master和备用Master WebUI页面，观察状态

## 注意点
- 主备切换过程中不能提交Application。
- 主备切换过程中不影响已经在集群中运行的Application。因为Spark是粗粒度资源调度。

## 测试验证
提交SparkPi程序，kill主Master观察现象。

```shell script
./spark-submit 
--master spark://node1:7077,node2:7077 
--class org.apache.spark.examples.SparkPi 
../lib/spark-examples-1.6.0-hadoop2.6.0.jar 
10000
```