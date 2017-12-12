package topK.airports;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopFifteenMapper2 extends Mapper<LongWritable, Text, IntWritable, Text>{

    //private final static IntWritable sales = new IntWritable();
    //private Text employee = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        
        String row[] = value.toString().split("\\t");
        //IntWritable movieId = new IntWritable(Integer.parseInt(row[0]));
        //Text airport = new Text(row[0]);
        
        String rating = row[1].trim();
        
        try {
            IntWritable rating_int = new IntWritable(Integer.parseInt(rating));
            context.write(rating_int, new Text(row[0]));

        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    
}