package smile.wide.algorithms.fang;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

//adapted from custom input format that generated random seeds by Shooltz

public class StructureInputFormat extends InputFormat<LongWritable, Void> {
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int count = conf.getInt("nvar", 0);//TODO MDJ We need some way to get info here, but newer compiled/older run is a problem.
		ArrayList<InputSplit> out = new ArrayList<InputSplit>(count);
		for (int i = 0; i < count; i ++) {
			out.add(new StructureSplit(i));
		}
		return out;
	}

	@Override
	public RecordReader<LongWritable, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new StructureReader((StructureSplit)split);
	}

	private static class StructureReader extends RecordReader<LongWritable, Void> {
		public StructureReader(StructureSplit split) {
			this.split = split;
		}
		
		private StructureSplit split;
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return new LongWritable(split.getValue());
		}

		@Override
		public Void getCurrentValue() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean ret = next;
			next = false;
			return ret;
		}
		
		private boolean next = true;
	}
	
	private static class StructureSplit extends InputSplit implements Writable {
		@SuppressWarnings("unused")
		public StructureSplit() {
		}
		
		public StructureSplit(long value) {
			this.value = value;
		}
		
		public long getValue() {
			return value;
		}
		
		private long value;
		
		@Override
		public long getLength() throws IOException, InterruptedException {
			return 8;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return EMPTY;
		}
		
		private static String[] EMPTY = new String[]{};

		@Override
		public void readFields(DataInput input) throws IOException {
			value = input.readLong();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeLong(value);
		}
	}


}
