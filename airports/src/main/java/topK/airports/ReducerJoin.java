package topK.airports;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer<Text, Text, Text, Text> {

	public static final Text EMPTY_TEXT = new Text("");
	private Text tmp = new Text();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private String joinType = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		joinType = context.getConfiguration().get("join.type");
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		listA.clear();
		listB.clear();

		while (values.iterator().hasNext()) {
			tmp = values.iterator().next();
			if ((Character.toString((char) tmp.charAt(0)).equals("A"))) {
				listA.add(new Text(tmp.toString().substring(1)));
			}
			if ((Character.toString((char) tmp.charAt(0)).equals("B"))) {
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}

		//System.out.println("Size of A: " + listA.size() + " Size of B: " + listB.size());
		executeJoin(context);

	}

	private void executeJoin(Context context) throws IOException, InterruptedException {
		if (joinType.equalsIgnoreCase("inner")) {
//			for (Text B : listB) {
//				System.out.println(B);
//			}
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text A : listA) {
					for (Text B : listB) {
						System.out.println("ListA B contains : " + A + " " + B);
						context.write(A, B);
					}
				}
			}
		} else if (joinType.equalsIgnoreCase("leftouter")) {
			for (Text A : listA) {
				if (!listB.isEmpty()) {
					for (Text B : listB) {
						context.write(A, B);
					}
				} else {
					context.write(A, EMPTY_TEXT);
				}
			}
		}
	}

}
