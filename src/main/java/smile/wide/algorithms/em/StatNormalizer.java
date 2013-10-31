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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import smile.Network;
import smile.wide.hadoop.io.DoubleArrayWritable;

/**
 * Performs the summation and normalization of the sufficient statistics
 * @author shooltz@shooltz.com
 *
 */
public class StatNormalizer extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		net = new Network();
		String netFile = context.getConfiguration().get(ConfKeys.WORK_NET_FILE);
		net.readFile(netFile);

		outcomeCounts = new HashMap<Integer, Integer>();
		for (int h = net.getFirstNode(); h >= 0; h = net.getNextNode(h)) {
			outcomeCounts.put(h, net.getOutcomeCount(h));
		}
	}

	/**
	 * Sums and normalizes the stats
	 */
	@Override
	public void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
		double[] totals = null; 
		for (DoubleArrayWritable v: values) {
			Writable[] counts = v.get();
			if (totals == null) {
				totals = new double[counts.length];
			}
			
			for (int i = 0; i < totals.length; i ++) {
				DoubleWritable dw = (DoubleWritable)counts[i];
				totals[i] += dw.get();
			}
		}

		int handle = key.get();
		if (handle >= 0) {
			int outcomes = outcomeCounts.get(handle);
			for (int i = 0; i < totals.length; i += outcomes) {
				double colTotal = 0;
				for (int j = 0; j < outcomes; j ++) {
					colTotal += totals[i + j];
				}
				if (colTotal > 0) {
					for (int j = 0; j < outcomes; j ++) {
						totals[i + j] /= colTotal;
					}
				} else {
					double uniform = 1.0 / outcomes;
					for (int j = 0; j < outcomes; j ++) {
						totals[i + j] = uniform;
					}
				}
			}
		}

		StringBuilder s = new StringBuilder(totals.length * 10);
		
		for (int i = 0; i < totals.length; i ++) {
			if (i > 0) {
				s.append(' ');
			}
			s.append(totals[i]);
		}
		context.write(key, new Text(s.toString()));
	}

	private Map<Integer, Integer> outcomeCounts;
	private Network net;
}