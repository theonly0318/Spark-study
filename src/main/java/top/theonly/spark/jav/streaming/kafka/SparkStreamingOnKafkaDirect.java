package top.theonly.spark.jav.streaming.kafka;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * SparkStreaming2.3版本 读取kafka 中数据 ：
 *  1.采用了新的消费者api实现，类似于1.6中SparkStreaming 读取 kafka Direct模式。并行度 一样。
 *  2.因为采用了新的消费者api实现，所以相对于1.6的Direct模式【simple api实现】 ，api使用上有很大差别。未来这种api有可能继续变化
 *  3.kafka中有两个参数：
 *      heartbeat.interval.ms：这个值代表 kafka集群与消费者之间的心跳间隔时间，kafka 集群确保消费者保持连接的心跳通信时间间隔。
 *             这个时间默认是3s.这个值必须设置的比session.timeout.ms 小，一般设置不大于 session.timeout.ms  的1/3
 *      session.timeout.ms ：
 *             这个值代表消费者与kafka之间的session 会话超时时间，如果在这个时间内，
 *             kafka 没有接收到消费者的心跳【heartbeat.interval.ms 控制】，那么kafka将移除当前的消费者。这个时间默认是30s。
 *             这个时间位于配置 group.min.session.timeout.ms【6s】 和 group.max.session.timeout.ms【300s】之间的一个参数,
 *             如果SparkSteaming 批次间隔时间大于5分钟，也就是大于300s,那么就要相应的调大group.max.session.timeout.ms 这个值。
 *  4.大多数情况下，SparkStreaming读取数据使用
 *          LocationStrategies.PreferConsistent 这种策略，这种策略会将分区均匀的分布在集群的Executor之间。
 *    如果Executor在kafka 集群中的某些节点上，可以使用
 *          LocationStrategies.PreferBrokers 这种策略，那么当前这个Executor 中的数据会来自当前broker节点。
 *    如果节点之间的分区有明显的分布不均，可以使用
 *          LocationStrategies.PreferFixed 这种策略,可以通过一个map 指定将topic分区分布在哪些节点中。
 *
 *  5.新的消费者api 可以将kafka 中的消息预读取到缓存区中，默认大小为64k。默认缓存区在 Executor 中，加快处理数据速度。
 *     可以通过参数 spark.streaming.kafka.consumer.cache.maxCapacity 来增大，也可以通过spark.streaming.kafka.consumer.cache.enabled 设置成false 关闭缓存机制。
 *     "注意：官网中描述这里建议关闭，在读取kafka时如果开启会有重复读取同一个topic partition 消息的问题，报错：KafkaConsumer is not safe for multi-threaded access"
 * *
 *  6.关于消费者offset
 *    1).如果设置了checkpoint ,那么offset 将会存储在checkpoint中。可以利用checkpoint恢复offset ， getOrCreate 方法获取
 *     这种有缺点: 第一，当从checkpoint中恢复数据时，有可能造成重复的消费。
 *                第二，当代码逻辑改变时，无法从checkpoint中来恢复offset.
 *    2).依靠kafka 来存储消费者offset,kafka 中有一个特殊的topic 来存储消费者offset。新的消费者api中，会定期自动提交offset。这种情况有可能也不是我们想要的，
 *       因为有可能消费者自动提交了offset,但是后期SparkStreaming 没有将接收来的数据及时处理保存。这里也就是为什么会在配置中将enable.auto.commit 设置成false的原因。
 *       这种消费模式也称最多消费一次，默认sparkStreaming 拉取到数据之后就可以更新offset,无论是否消费成功。自动提交offset的频率由参数auto.commit.interval.ms 决定，默认5s。
 *       *如果我们能保证完全处理完业务之后，可以后期异步的手动提交消费者offset.
 *       注意：这种模式也有弊端，这种将offset存储在kafka中方式，参数offsets.retention.minutes=1440控制offset是否过期删除，默认将offset信息保存一天，
 *       如果停机没有消费达到时长，存储在kafka中的消费者组会被清空，offset也就被清除了。
 *    3).自己存储offset,这样在处理逻辑时，保证数据处理的事务，如果处理数据失败，就不保存offset，处理数据成功则保存offset.这样可以做到精准的处理一次处理数据。
 *
 * @author theonly
 */
public class SparkStreamingOnKafkaDirect {
    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf();
        // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
        conf.setMaster("local[2]");
        // 设置应用程序名称
        conf.setAppName("SparkStreamingReadSocket");
        // 创建StreamingContext（java中为JavaStreamingContext）
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        //
        Map<String, Object> kafkaParams = new HashMap<>();
        // kafka broker列表
        kafkaParams.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        // key，value的反序列化类
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        // 设置消费者组id
        kafkaParams.put("group.id", "my_consumer_group_1");
        /**
         * 如果Kafka中没有初始偏移量，或者服务器上不再存在当前偏移量（例如，因为该数据已被删除），该怎么办：
         *  earliest：自动将偏移量重置为最早的偏移量（有已提交的offset时，从提交的offset开始往后读，没有则从头开始读）
         *  latest：自动将偏移重置为最新偏移 (默认)
         *  none：如果未找到消费者组的先前偏移量，则向消费者抛出异常
         */
        kafkaParams.put("auto.offset.reset", "latest");
        // 如果为true， offset信息将在后台定期提交（默认5秒提交一次），false：消费者自己维护offset
        kafkaParams.put("enable.auto.commit", false);

        // 要消费的topic集合
        Collection<String> topics = Arrays.asList("topic1");
        // 创建DirectStream
        // 请注意，KafkaUtils包名：org.apache.spark.streaming.kafka010
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                // JavaStreamingContext
                streamingContext,
                // 本地策略
                LocationStrategies.PreferConsistent(),
                // 消费策略
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
        /**
         *  LocationStrategies.PreferConsistent
         */

        JavaPairDStream<String, String> ds1 = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        JavaDStream<String> ds2 = ds1.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
                return Arrays.asList(tuple2._2.split("\t")).iterator();
            }
        });
        JavaPairDStream<String, Integer> ds3 = ds2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> ds4 = ds3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        ds4.print();

        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> record) throws Exception {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) record.rdd()).offsetRanges();
                // 获取offset
                // 方式一：
//                record.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
//                    @Override
//                    public void call(Iterator<ConsumerRecord<String, String>> iterator) throws Exception {
//                        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                        System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//                    }
//                });
                // 方式二：
                for (OffsetRange o : offsetRanges) {
                    System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                }
                // 异步提交offset
                ((CanCommitOffsets)stream).commitAsync(offsetRanges);
            }
        });
    }
}
