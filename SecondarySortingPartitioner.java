package com.mapreduce.secondarysorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortingPartitioner
        extends Partitioner<SecondarySortingKeyClass, NullWritable>
{
    @Override
    public int getPartition(SecondarySortingKeyClass key, NullWritable value
    , int numReduceTasks)
    {
        return (key.getEmpDOBYear().hashCode() % numReduceTasks);
    }
}
