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
package smile.wide.algorithms.em;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import smile.Network;
import smile.wide.hadoop.io.DoubleArrayWritable;

/**
 * Calls SMILE to obtain sufficient stats and logarithmic likelihood for each data row in the input file
 * @author shooltz@shooltz.com
 *
 */
public class StatEstimator extends Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> {

	/**
	 * Initializes the structures used to improve the mapper performance
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		separator = conf.get(ConfKeys.SEPARATOR);
		missingValueToken = conf.get(ConfKeys.MISSING_TOKEN);
		skipFirstLine = conf.get(ConfKeys.IGNORE_FIRST_LINE) != null;
		
		
		String netFile = conf.get(ConfKeys.WORK_NET_FILE);
		net = new Network();
		net.readFile(netFile);
		net.clearAllTargets();
		
		defSizes = new HashMap<Integer, Integer>();
		
		Map<String, Integer> idMap = new HashMap<String, Integer>();
		totalCptSize = 0;
		for (int h = net.getFirstNode(); h >= 0; h = net.getNextNode(h)) {
			int len = net.getNodeDefinition(h).length;
			defSizes.put(h, len);
			totalCptSize += len;
			idMap.put(net.getNodeId(h), h);
		}
		
		String[] columns = conf.get(ConfKeys.COLUMNS).split(separator);
		
		colMap = new HashMap<Integer, Integer>();
		for (int i = 0; i < columns.length; i ++) {
			String id = columns[i];
			if (idMap.containsKey(id)) {
				colMap.put(i, net.getNode(id));
			}
		}
	}

	/**
	 * Calls into SMILE to obtain sufficient statistics and log likelihood.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (skipFirstLine && (key.get() == 0)) {
			return; // header line ignored
		}
		
		String line = value.toString();
		net.clearAllEvidence();
		String[] values = line.split(separator);
		for (int i = 0; i < values.length; i ++) {
			if (colMap.containsKey(i)) {
				String v = values[i];
				if (!v.isEmpty() && !v.equals(missingValueToken)) {
					net.setEvidence(colMap.get(i), v);
				}
			}
		}
		
		double pe = net.probEvidence();

		double[] counts = new double[totalCptSize];
		net.distributedHelperEM(counts);

		DoubleWritable[] peOut = new DoubleWritable[1];
		peOut[0] = new DoubleWritable(Math.log(pe));
		context.write(new IntWritable(-1), new DoubleArrayWritable(peOut));
		
		int pos = 0;
		for (int h = net.getFirstNode(); h >= 0; h = net.getNextNode(h)) {
			int len = defSizes.get(h);
			DoubleWritable[] countOut = new DoubleWritable[len];	
			for (int i = 0; i < len; i ++) {
				countOut[i] = new DoubleWritable(counts[pos ++]);
			}
			context.write(new IntWritable(h), new DoubleArrayWritable(countOut));
		}
	}
	
	private String separator;
	private boolean skipFirstLine;
	private String missingValueToken;
	private Network net;
	private Map<Integer, Integer> colMap;
	private Map<Integer, Integer> defSizes;
	private int totalCptSize;
}
