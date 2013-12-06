package smile.wide.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class PairDoubleWritable implements Writable {
	DoubleWritable left = new DoubleWritable(0);
	DoubleWritable right = new DoubleWritable(0);

	public PairDoubleWritable() {
		left = new DoubleWritable(0);
		right = new DoubleWritable(0);
	}

	public PairDoubleWritable(double l, double r) {
		left = new DoubleWritable(l);
		right = new DoubleWritable(r);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
	}

	public double getLeft() {
		return left.get();
	}

	public void setLeft(double l) {
		left.set(l);
	}
	
	public double getRight() {
		return right.get();
	}

	public void setRight(double r) {
		right.set(r);
	}
	@Override
	public String toString() {
		return left + "\t" + right;
	}
}

