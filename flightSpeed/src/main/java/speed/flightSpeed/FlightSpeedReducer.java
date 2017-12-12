package speed.flightSpeed;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightSpeedReducer extends Reducer<Text, CustomWritable, Text, FloatWritable> {

	public void reduce(Text key, Iterable<CustomWritable> values, Context context)
			throws IOException, InterruptedException {

		CustomWritable custom = new CustomWritable();
		float distance = 0;
		float airtime = 0;
		float speed = 0;
		float avgSpeed = 0;
		int count = 0;

		for (CustomWritable val : values) {
			distance = val.getDistance();
			airtime = val.getAirTime();
			speed += (distance / airtime) * 60;
			count++;

		}

		if (count != 0) {
			avgSpeed = speed / count;
			custom.setSpeed(avgSpeed);
			context.write(key, new FloatWritable(custom.getSpeed()));

		}
	}

}
