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

//Code for custom input file that returns random seeds (Written by Tomek Sowinski)

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
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
public class RandSeedInputFormat extends InputFormat<LongWritable, Void> {
	/** Number of seeds to be generated */
	public static final String CONFKEY_SEED_COUNT = "SEED_COUNT";
	/** Meta seed used to generate the (output) seeds*/
	public static final String CONFKEY_METASEED = "SEED_METASEED";
	/** Number of burn iterations before seeds are actually used*/
	public static final String CONFKEY_WARMUP_ITER = "SEED_WARMUP_ITER";

	/**meta seed value obtained from http://www.fourmilab.ch/hotbits/generate.html */
	private static final String DEFAULT_METASEED =
		"2457FF2F0FC40679588423B44275C6DD3C55ABA0A5D083969DB584927BD1520B6A1122F957F19F9010D8D85AB032F63924DD6D9F7E0D87F69E45C3AAF3A1A776";
	
	/** Initializes Mersenne Twister random number generator object
	 * @param metaSeed meta seed to be used
	 * @param number of burn iterations before generator is "warmed up"
	 * @return MTrandom random generator object
	 * */
	private MTRandom createTwister(String metaSeed, int warmupIterCount) {
		BigInteger big = new BigInteger(metaSeed, 16);
		MTRandom mt = new MTRandom(big.toByteArray());
		for (int i = 0; i < warmupIterCount; i ++) {
			mt.nextInt();
		}
		return mt;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration(); 

		String metaSeed = conf.get(CONFKEY_METASEED);
		if (metaSeed == null || metaSeed.length() < 8) {
			metaSeed = DEFAULT_METASEED;
		}

		int warmupIter = conf.getInt(CONFKEY_WARMUP_ITER, 1);
		
		int len = metaSeed.length();
		MTRandom randStartPoint = createTwister(metaSeed.substring(0, len / 2), warmupIter);
		MTRandom randParamBase = createTwister(metaSeed.substring(len / 2), warmupIter);
		
		int count = conf.getInt(CONFKEY_SEED_COUNT, 1);
		ArrayList<InputSplit> out = new ArrayList<InputSplit>(count);
		for (int i = 0; i < count; i ++) {
			long r1 = randStartPoint.nextInt() & 0x00000000ffffffffL;
			long r2 = randParamBase.nextInt() & 0x00000000ffffffffL;
			long r = (r1 << 32) | r2;
			out.add(new RandSeedSplit(r));
		}
		return out;
	}

	@Override
	public RecordReader<LongWritable, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new RandSeedReader((RandSeedSplit)split);
	}

	private static class RandSeedReader extends RecordReader<LongWritable, Void> {
		public RandSeedReader(RandSeedSplit split) {
			this.split = split;
		}
		
		private RandSeedSplit split;
		
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
	
	private static class RandSeedSplit extends InputSplit implements Writable {
		@SuppressWarnings("unused")
		public RandSeedSplit() {
		}
		
		public RandSeedSplit(long value) {
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
