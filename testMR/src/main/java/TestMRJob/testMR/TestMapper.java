package TestMRJob.testMR;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TestMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] carrierList = line.split(",");

		String carrier = carrierList[8];	
		
		try {
            if (key.get() == 0 && value.toString().contains("UniqueCarrier") /*Some condition satisfying it is header*/)
                return;
            else {
            	context.write(new Text(carrier), one);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

	}

}
