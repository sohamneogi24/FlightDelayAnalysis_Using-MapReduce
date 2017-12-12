package speed.flightSpeed;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomWritable implements Writable {

	private float distance;
	private float airTime;
	private float speed;

	public CustomWritable() {
	}

	public CustomWritable(float distance, float airTime, float speed) {
		this.distance = distance;
		this.airTime = airTime;
		this.speed = speed;
	}

	
	public float getDistance() {
		return distance;
	}

	public void setDistance(float distance) {
		this.distance = distance;
	}

	public float getAirTime() {
		return airTime;
	}

	public void setAirTime(float airTime) {
		this.airTime = airTime;
	}

	public float getSpeed() {
		return speed;
	}

	public void setSpeed(float speed) {
		this.speed = speed;
	}


	public void readFields(DataInput in) throws IOException {
		distance = in.readFloat();
		airTime = in.readFloat();
		speed = in.readFloat();

	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(distance);
		out.writeFloat(airTime);
		out.writeFloat(speed);
	}

	@Override
	public String toString() {
		return (distance + " " + airTime + " " + speed);
	}

}