package topK.airports;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class TopFifteenReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        //int count=0;
        int sum=0; 
        //double avg=0;
        
        for(IntWritable val : values){
            sum += val.get();
            //++count;
        }
       
        //avg=sum/count;
        result.set(sum);
        context.write(key, result);
    }
    
    
    
}
