package com.mapreduce.secondarysorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

public class SecondarySortingJob extends Configured implements Tool{

    public void main(String[] args)
    {
        if (args.length <2)
        {
            System.out.println("Map Reduce Job Usage : " + SecondarySortingJob.class.getName() + ". Please provide valid input");
            return;
        }

        Configuration conf = new Configuration(Boolean.TRUE);

        try
        {
            int i = ToolRunner.run(conf, new SecondarySortingJob(),args);

            if (i==0)
            {
                System.out.println("Success");
            }
            else
            {
                System.out.println("Failed");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = super.getConf();
        Job SecondarySortingJob = Job.getInstance(conf,this.getClass().getName());

        FileInputFormat.setInputPaths(SecondarySortingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(SecondarySortingJob, new Path(args[1]));

        SecondarySortingJob.setJarByClass(SecondarySortingJob.class);

        SecondarySortingJob.setMapperClass(SecondarySortingMapper.class);
        SecondarySortingJob.setMapOutputKeyClass(SecondarySortingKeyClass.class);
        SecondarySortingJob.setMapOutputKeyClass(NullWritable.class);

        SecondarySortingJob.setPartitionerClass(SecondarySortingPartitioner.class);

        SecondarySortingJob.setSortComparatorClass(SecondarySortingKeyComparator.class);

        SecondarySortingJob.setGroupingComparatorClass(SecondarySortingGroupComparator.class);

        SecondarySortingJob.setReducerClass(SecondarySortingReducer.class);
        SecondarySortingJob.setOutputKeyClass(SecondarySortingKeyClass.class);
        SecondarySortingJob.setOutputValueClass(NullWritable.class);

        SecondarySortingJob.setNumReduceTasks(4);


        //SecondarySortingJob.setCom

        return 0;
    }


}
