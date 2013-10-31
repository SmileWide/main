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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import smile.wide.hadoop.io.DoubleArrayWritable;

/**
 * Adds the values (counts) from the sufficient statistics output. Essential for adequate performance.
 * @author shooltz@shooltz.com
 *
 */
public class StatCombiner extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
	
	/**
	 * Performs the summing.
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

		DoubleWritable[] totalsOut = new DoubleWritable[totals.length];
		for (int i = 0; i < totals.length; i ++) {
			totalsOut[i] = new DoubleWritable(totals[i]);
		}
		
		context.write(key, new DoubleArrayWritable(totalsOut));
	}
	
}