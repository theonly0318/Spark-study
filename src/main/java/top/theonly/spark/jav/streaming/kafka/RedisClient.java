package top.theonly.spark.jav.streaming.kafka;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;

public class RedisClient {

    private static String host = "node4";
    private static int port = 6379;
    private static int timeout = 30000;

    /**
     * JedisPool是一个连接池，既可以保证线程安全，又可以保证了较高的效率。
     */
    public static JedisPool jedisPool() {
        return new JedisPool(new GenericObjectPoolConfig(), host, port, timeout);
    }
}
