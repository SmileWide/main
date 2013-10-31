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
package smile.wide.counter;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import smile.Network;

public class ReduceCounter extends Reducer<BytesWritable, DoubleWritable, Text, DoubleWritable> {
	private Parameters params;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		params = new Parameters(context.getConfiguration());
	}

	public void reduce(BytesWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double total = 0;
		for (DoubleWritable p: values) {
			total += p.get();
		}
		
		byte[] bits = key.getBytes();
		BitBuffer buf = new BitBuffer(bits, bits.length * 8);
		
		Network net = params.getNet();
		
		StringBuilder out = new StringBuilder();
		
		int[][] families = params.getFamilies();
		int pos = Parameters.bitCount(families.length);
		int familyIndex = buf.getBits(0, pos);
		out.append(familyIndex);
		int[] keyFamily = families[familyIndex];
		for (int i = 0; i < keyFamily.length; i ++) {
			out.append(' ');
			int handle = keyFamily[i];
			int bitsPerNode = Parameters.bitCount(net.getOutcomeCount(handle));
			int outcomeIndex = buf.getBits(pos, bitsPerNode);
			out.append(net.getOutcomeId(handle, outcomeIndex));
			pos += bitsPerNode;
		}
	
		context.write(new Text(out.toString()), new DoubleWritable(total));
	}
}
