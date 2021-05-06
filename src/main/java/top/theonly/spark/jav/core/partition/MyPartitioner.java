package top.theonly.spark.jav.core.partition;

import org.apache.spark.Partitioner;

import java.io.Serializable;

/**
 * 自定义Spark分区其器
 * @author theonly
 */
public class MyPartitioner extends Partitioner implements Serializable {
    @Override
    public int numPartitions() {
        return 6;
    }

    @Override
    public int getPartition(Object key) {
        Integer k = Integer.valueOf(key.toString());
        return k % numPartitions();
    }
}
