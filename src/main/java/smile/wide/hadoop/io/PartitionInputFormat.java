/*
             Licensed to the DARPA XDATA project.
       DARPA XDATA licenses this file to you under the 
         Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
           You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
                 either express or implied.                    
   See the License for the specific language governing
     permissions and limitations under the License.
*/
package smile.wide.hadoop.io;

//Code for custom input file that returns random seeds 
//(Original (RandSeedInputFormat) written by Tomek Sowinski, 
//adapted by Martijn de Jongh)

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

/** Code for custom input file that returns random seeds 
 * @author Tomek Sowinski
 */
public class PartitionInputFormat extends InputFormat<LongWritable, Void> {
	public static final String CONFKEY_MAP_COUNT = "MAP_COUNT";
	public static final String CONFKEY_INTERVAL_COUNT = "INTERVAL_COUNT";

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration(); 
		int count = conf.getInt(CONFKEY_MAP_COUNT, 1);
		int k = conf.getInt(CONFKEY_INTERVAL_COUNT,1);
		ArrayList<InputSplit> out = new ArrayList<InputSplit>(count);
		int start=0;
		for (int i = 0; i < count; i++) {
			out.add(new PartitionSplit(start));
			start+=k;
		}
		return out;
	}

	@Override
	public RecordReader<LongWritable, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new PartitionReader((PartitionSplit)split);
	}

	private static class PartitionReader extends RecordReader<LongWritable, Void> {
		public PartitionReader(PartitionSplit split) {
			this.split = split;
		}
		
		private PartitionSplit split;
		
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
	
	private static class PartitionSplit extends InputSplit implements Writable {
		@SuppressWarnings("unused")
		public PartitionSplit() {
		}
		
		public PartitionSplit(long value) {
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
