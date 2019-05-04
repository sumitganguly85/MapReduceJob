package com.mapreduce.secondarysorting;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.lang.StringUtils;
import java.io.IOException;


public class SecondarySortingMapper extends Mapper<LongWritable, Text, SecondarySortingKeyClass, NullWritable>
{
    private String strEmpDOB = "";
    private String strEmpDOBYear = "";
    private String strDojFNameLNameEmpId = "";
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        final String strValue = value.toString();
        if (!StringUtils.isEmpty(strValue))
        {
            String arrEmpDetails[] = strValue.split(",");
            //Get Emp Hire Date From Data
            strEmpDOB = arrEmpDetails[1].toString();
            strEmpDOBYear = strEmpDOB.split("-")[0].toString();
            strDojFNameLNameEmpId = arrEmpDetails[5].toString() + "\t" + arrEmpDetails[2].toString() + "\t" + arrEmpDetails[3].toString() + arrEmpDetails[0].toString();
            context.write(new SecondarySortingKeyClass(strEmpDOBYear,strDojFNameLNameEmpId), NullWritable.get());
        }

    }
}
