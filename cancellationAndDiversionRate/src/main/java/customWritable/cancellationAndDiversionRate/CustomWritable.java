package customWritable.cancellationAndDiversionRate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomWritable implements Writable {

	private float cancellation;
	private float diversion;

	public CustomWritable() {
	}

	public CustomWritable(float cancellation, float diversion) {
		this.cancellation = cancellation;
		this.diversion = diversion;
	}

	public float getCancellation() {
		return cancellation;
	}

	public float getDiversion() {
		return diversion;
	}

	public void setCancellation(float cancellation) {
		this.cancellation = cancellation;
	}

	public void setDiversion(float diversion) {
		this.diversion = diversion;
	}

	public void readFields(DataInput in) throws IOException {
		cancellation = in.readFloat();
		diversion = in.readFloat();

	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(cancellation);
		out.writeFloat(diversion);
	}

	@Override
	public String toString() {
		return (cancellation + " " + diversion);
	}

}