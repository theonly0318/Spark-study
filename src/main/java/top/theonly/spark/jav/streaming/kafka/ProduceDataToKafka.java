package top.theonly.spark.jav.streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 向Kafka生产消息
 * @author theonly
 */
public class ProduceDataToKafka {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // kafka broker列表
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        // key和value的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int counter = 0;
        int keyFlag = 0;

        while(true){
            counter +=1;
            keyFlag +=1;
            String content = userlogs();
            producer.send(new ProducerRecord<String, String>("topic1", "key-"+keyFlag, content));
            if(0 == counter%100){
                counter = 0;
                Thread.sleep(5000);
            }
        }

    }

    public static String userlogs() {
        StringBuffer userLogBuffer = new StringBuffer("");
        long timestamp = System.currentTimeMillis();
        long userID = 0L;
        long pageID = 0L;

        Random random = new Random();
        //随机生成的用户ID
        userID = random.nextInt(2000);
        //随机生成的页面ID
        pageID =  random.nextInt(2000);

        //随机生成Channel
        String[] channelNames = new String[]{"Spark", "Scala","Kafka","Flink","Hadoop","Storm","Hive","Impala","HBase","ML"};
        String channel = channelNames[random.nextInt(10)];

        String[] actionNames = {"View", "Register"};
        //随机生成action行为
        String action = actionNames[random.nextInt(2)];

        String dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        userLogBuffer.append(dateToday)
                .append("\t")
                .append(timestamp)
                .append("\t")
                .append(userID)
                .append("\t")
                .append(pageID)
                .append("\t")
                .append(channel)
                .append("\t")
                .append(action);
        System.out.println(userLogBuffer.toString());
        return userLogBuffer.toString();
    }
}
