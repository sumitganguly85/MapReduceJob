package com.mapreduce.secondarysorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortingKeyComparator extends WritableComparator{

    protected SecondarySortingKeyComparator()
    {
        super(SecondarySortingKeyClass.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        SecondarySortingKeyClass key1 = (SecondarySortingKeyClass) w1;
        SecondarySortingKeyClass key2 = (SecondarySortingKeyClass) w2;
        int cmpResDOBYear = key1.getEmpDOBYear().compareTo(key2.getEmpDOBYear());

        if (cmpResDOBYear == 0) // Same Emp DOB Year Same
        {
            return -key1.getDojFNameLNameEmpId().compareTo(key2.getDojFNameLNameEmpId());
        }

        return cmpResDOBYear;
    }
}
