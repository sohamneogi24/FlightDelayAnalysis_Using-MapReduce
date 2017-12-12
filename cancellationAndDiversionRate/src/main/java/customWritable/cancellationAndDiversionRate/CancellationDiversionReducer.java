package customWritable.cancellationAndDiversionRate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CancellationDiversionReducer extends Reducer<Text, CustomWritable, Text, CustomWritable> {

	public void reduce(Text key, Iterable<CustomWritable> values, Context context)
			throws IOException, InterruptedException {

		CustomWritable custom = new CustomWritable();

		float sumOfCancellation = 0;
		float sumOfDiversion = 0;

		float totalofCancellation = 0;
		float totalofDiversion = 0;

		float percentCancellation = 0;
		float percentDiversion = 0;

		for (CustomWritable val : values) {

			sumOfCancellation += val.getCancellation();
			totalofCancellation++;
			sumOfDiversion += val.getDiversion();
			totalofDiversion++;
		}

		percentCancellation = (sumOfCancellation / totalofCancellation) * 100;
		percentDiversion = (sumOfDiversion / totalofDiversion) * 100;

//		System.out.println("**********************************************");
//		System.out.println("TOTAL CANCELLATION " + sumOfCancellation);
//		System.out.println("TOTAL DIVERSION " + totalofCancellation);
//		System.out.println("**********************************************");

		custom.setCancellation(percentCancellation);
		custom.setDiversion(percentDiversion);
		context.write(key, custom);

	}
}