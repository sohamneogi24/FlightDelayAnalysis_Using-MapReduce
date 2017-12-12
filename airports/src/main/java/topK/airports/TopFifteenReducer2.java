package topK.airports;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopFifteenReducer2 extends Reducer<IntWritable, Text, Text, IntWritable>{

    private static int count = 15; 
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
        for(Text val : values){
            if(count>0){
                context.write(val, key);
                --count;
            }    
            else
               break; 
        }
        
    }

        
    
}