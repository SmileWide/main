package smile.wide.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.IntPairWritable;

public final class ItestPairWritable implements WritableComparable<ItestPairWritable> {

	  private IntPairWritable first;
	  private IntArrayWritable second;
	  
	  public ItestPairWritable() {
	    set(new IntPairWritable(), new IntArrayWritable());
	  }
	  
	  public ItestPairWritable(int left, int right, IntWritable[] second) {
	    set(new IntPairWritable(left,right), new IntArrayWritable(second));
	  }
	  
	  public ItestPairWritable(IntPairWritable first, IntArrayWritable second) {
	    set(first, second);
	  }
	  
	  public void set(IntPairWritable first, IntArrayWritable second) {
	    this.first = first;
	    this.second = second;
	  }
	  
	  public IntPairWritable getFirst() {
	    return first;
	  }

	  public IntArrayWritable getSecond() {
	    return second;
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    first.write(out);
	    second.write(out);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    first.readFields(in);
	    second.readFields(in);
	  }
	  
	  @Override
	  public int hashCode() {
	    return first.hashCode() * 163 + second.hashCode();
	  }
	  
	  @Override
	  public boolean equals(Object o) {
	    if (o instanceof ItestPairWritable) {
	      ItestPairWritable tp = (ItestPairWritable) o;
	      return first.equals(tp.first) && second.equals(tp.second);
	    }
	    return false;
	  }

	  @Override
	  public String toString() {
	    return first + "\t" + second;
	  }
	  
	  @Override
	  public int compareTo(ItestPairWritable tp) {
	    int cmp = first.compareTo(tp.first);
	    if (cmp != 0) {
	      return cmp;
	    }
	    return second.compareTo(tp.second);
	  }
}