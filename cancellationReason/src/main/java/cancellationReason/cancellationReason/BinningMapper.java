package cancellationReason.cancellationReason;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> mos = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs(context);
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String row[] = value.toString().split(",");
		if (!value.toString().contains("UniqueCarrier")) {
			if (!row[21].equalsIgnoreCase("0")) {
				String cancellationCode = row[22];

				if (cancellationCode.equalsIgnoreCase("A"))
					mos.write("bins", value, NullWritable.get(), "Carrier-cancellation");
				if (cancellationCode.equalsIgnoreCase("B"))
					mos.write("bins", value, NullWritable.get(), "Weather-cancellation");
				if (cancellationCode.equalsIgnoreCase("C"))
					mos.write("bins", value, NullWritable.get(), "NAS-cancellation");
				if (cancellationCode.equalsIgnoreCase("D"))
					mos.write("bins", value, NullWritable.get(), "Security-cancellation");
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
