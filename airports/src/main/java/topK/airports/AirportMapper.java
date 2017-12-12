package topK.airports;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportMapper extends Mapper<Object, Text, Text, Text>{
    
    private Text outkey= new Text();
    private Text outValue= new Text();
    
    @Override
     public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
      
        String aiportList[] = value.toString().split(",");
        
        if (!value.toString().contains("iata")) {
            String iata = aiportList[0].replace("\"", "");
            //System.out.println("Airport name ->" + iata);
            if(iata!=null){
                outkey.set(iata);
                outValue.set("B"+ value.toString().replace("\"", "").replace(",", " "));
                context.write(outkey, outValue);
            }
        }
    }
}