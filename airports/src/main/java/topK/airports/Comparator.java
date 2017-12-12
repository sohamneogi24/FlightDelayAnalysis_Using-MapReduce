package topK.airports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class Comparator extends WritableComparator {


    protected Comparator()
    {
        super(IntWritable.class,true);
    }

    @Override
    public int compare(WritableComparable w1,WritableComparable w2)
    {

    	IntWritable ip1 = (IntWritable) w1;
    	IntWritable ip2 = (IntWritable) w2;
        int cmp = -1 * ip1.compareTo(ip2);
        return cmp;

    }


}