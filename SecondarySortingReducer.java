package com.mapreduce.secondarysorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortingReducer extends Reducer<SecondarySortingKeyClass, NullWritable, SecondarySortingKeyClass, NullWritable>
{
    @Override
    public void reduce(SecondarySortingKeyClass key, Iterable<NullWritable> values,
                       Context context) throws IOException, InterruptedException
    {
        for(NullWritable value: values)
        {
            context.write(key, NullWritable.get());
        }
    }
}
