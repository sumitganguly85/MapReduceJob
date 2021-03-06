package com.mapreduce.secondarysorting;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondarySortingKeyClass
        implements Writable,
        WritableComparable<SecondarySortingKeyClass>{

    //Declare class variable for Emp Hire Date and Composite of DOj, Fname, LName, Emp Id etc
    private Text strEmpDOBYear;
    private Text strDojFNameLNameEmpId;

    //Set values for class variables
    public void set(Text emphiredate, Text dojfnamelnameempid)
    {
        this.strEmpDOBYear = emphiredate;
        this.strDojFNameLNameEmpId = dojfnamelnameempid;
    }

        //Get values for Hire Date
    public Text getEmpDOBYear()
    {
        return this.strEmpDOBYear;
    }

    //Get values for composite of Doj, Fname, Lname and Emp Id
    public Text getDojFNameLNameEmpId()
    {
        return this.strDojFNameLNameEmpId;
    }

    //Override default Constructor , will just create default key set
    public SecondarySortingKeyClass()
    {
        set(new Text(), new Text());
    }

    //Override Constructor with user provided first and second key in String Format, will just create default key set
    public SecondarySortingKeyClass(String empdobyear, String dojfnamelnameempid)
    {
        set(new Text(empdobyear), new Text(dojfnamelnameempid));
    }

    //Override Constructor with user provided first and second key in Text Format, will just create default key set
    public SecondarySortingKeyClass(Text empdobyear, Text dojfnamelnameempid)
    {
        set(empdobyear,dojfnamelnameempid);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        this.strEmpDOBYear.write(out);
        this.strDojFNameLNameEmpId.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.strEmpDOBYear.readFields(in);
        this.strDojFNameLNameEmpId.readFields(in);
    }


    @Override
    public int compareTo(SecondarySortingKeyClass sskc)
    {
        int cmp = this.strEmpDOBYear.compareTo(sskc.strEmpDOBYear);
        if (cmp != 0)
        {
            return cmp;
        }

        return this.strDojFNameLNameEmpId.compareTo(sskc.strDojFNameLNameEmpId);
    }


}
