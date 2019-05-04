package com.mapreduce.secondarysorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortingGroupComparator extends WritableComparator{

    protected SecondarySortingGroupComparator()
    {
        super(SecondarySortingKeyClass.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        SecondarySortingKeyClass key1 = (SecondarySortingKeyClass) w1;
        SecondarySortingKeyClass key2 = (SecondarySortingKeyClass) w2;
        return key1.getEmpDOBYear().compareTo(key2.getEmpDOBYear());
    }
}
