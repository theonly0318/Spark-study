package top.theonly.spark.jav.streaming.kafka;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

/**
 * 使用Redis手动管理kafka offset
 * @author theonly
 */
public class ManageOffsetUseRedis {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf();
        // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
        conf.setMaster("local[2]");
        // 设置应用程序名称
        conf.setAppName("SparkStreamingReadSocket");
        //设置每个分区每秒读取多少条数据
        conf.set("spark.streaming.kafka.maxRatePerPartition","10");

        // 创建StreamingContext（java中为JavaStreamingContext）
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        // kafka 消费者的一些配置参数
        Map<String, Object> kafkaParams = new HashMap<>();
        // kafka broker列表
        kafkaParams.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        // key，value的反序列化类
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        // 设置消费者组id
        kafkaParams.put("group.id", "my_consumer_group_1");
        // 可以不设置，应为手动维护了offset
        kafkaParams.put("auto.offset.reset", "latest");
        // 如果为true， offset信息将在后台定期提交（默认5秒提交一次），false：消费者自己维护offset
        kafkaParams.put("enable.auto.commit", false);

        /**
         * 从redis中获取topic的offset
         */
        Map<TopicPartition, Long> topicFromOffsets = getOffsetFromRedis(0, "topic1");

        // 创建DirectStream
        // 请注意，KafkaUtils包名：org.apache.spark.streaming.kafka010
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Assign(topicFromOffsets.keySet(), kafkaParams, topicFromOffsets)
        );

//        JavaDStream<String> map = stream.map(new Function<ConsumerRecord<String, String>, String>() {
//            @Override
//            public String call(ConsumerRecord<String, String> cr) throws Exception {
//                String topic = cr.topic();
//                int partition = cr.partition();
//                long offset = cr.offset();
//                return null;
//            }
//        });

        // 业务逻辑
        JavaPairDStream<String, Integer> resDS = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .flatMap(tp -> Arrays.asList(tp._2.split("\t")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(((v1, v2) -> v1 + v2));
        resDS.print();

        stream.foreachRDD(record -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) record.rdd()).offsetRanges();
            /**
             * 将当前批次最后的所有分区offsets  Redis中保存到
             */
            saveOffsetToRedis(0, offsetRanges);
        });
    }

    /**
     * 将消费者offset 保存到 Redis中
     *
     */
    public static void saveOffsetToRedis(Integer db, OffsetRange[] offsetRanges) {
        Jedis jedis = RedisClient.jedisPool().getResource();
        jedis.select(db);
        for (OffsetRange offsetRange : offsetRanges) {
            jedis.hset(offsetRange.topic(), String.valueOf(offsetRange.partition()), String.valueOf(offsetRange.untilOffset()));
        }
        System.out.println("保存成功");
        RedisClient.jedisPool().returnResource(jedis);
    }

    /**
     * 从Redis中获取保存的消费者offset
     * @param db
     * @param topic
     * @return
     */
    public static Map<TopicPartition, Long> getOffsetFromRedis(Integer db, String topic) {
        Jedis jedis = RedisClient.jedisPool().getResource();
        jedis.select(db);
        Map<String, String> map = jedis.hgetAll(topic);
        Map<TopicPartition, Long> topicOffsetRanges = new HashMap<>();

        if (map.isEmpty() || map == null) {
            topicOffsetRanges.put(new TopicPartition(topic, 0), 0L);
            topicOffsetRanges.put(new TopicPartition(topic, 1), 0L);
            topicOffsetRanges.put(new TopicPartition(topic, 2), 0L);
        }

        map.forEach((k,v) -> {
            TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(k));
            Long offset = Long.valueOf(v);
            topicOffsetRanges.put(topicPartition, offset);
        });
        return topicOffsetRanges;
    }
}
