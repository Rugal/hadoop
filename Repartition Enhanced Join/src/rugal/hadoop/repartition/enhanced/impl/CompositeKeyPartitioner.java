package rugal.hadoop.repartition.enhanced.impl;

import org.apache.hadoop.mapred.*;

public class CompositeKeyPartitioner implements
        Partitioner<CompositeKey, IntermediateData>
{

    @Override
    public int getPartition(CompositeKey key, IntermediateData value,
            int numPartitions) {
        return Math.abs(key.getKey().hashCode() * 127) % numPartitions;
    }

    @Override
    public void configure(JobConf job) {}
}
