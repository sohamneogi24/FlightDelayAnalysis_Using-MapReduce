package topK.airports;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayRatingMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outValue = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String airportList[] = value.toString().split("\t");
		String airport = airportList[0].replace("\"", "");

		if (airport != null) {
//			System.out.println("Airport name ->" + airport);
			outkey.set(airport);
			outValue.set("A" + value.toString().replace("\"", ""));
			context.write(outkey, outValue);
		}
	}

}
