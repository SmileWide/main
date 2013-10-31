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
import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import smile.Network;

public class MapCounter extends Mapper<LongWritable, Text, BytesWritable, DoubleWritable> {
	public static final String MAPPER_CONFKEY_DATA_ORDER = "dataorder";
	public static final String MAPPER_CONFKEY_FAMILIES = "families";
	
	private static final String MISSING_VALUE = "*"; 
	private static final DoubleWritable ONE = new DoubleWritable(1.0);
	
	private Parameters params;
	
	private void findMissingNodes(int[] family, ArrayList<Integer> missing) {
		for (int h: family) {
			if (!params.getNet().isRealEvidence(h)) {
				missing.add(h);
			}
		}
	}
	
	private boolean missingNodesExist(int[] family) {
		for (int h: family) {
			if (!params.getNet().isRealEvidence(h)) {
				return true;
			}
		}
		return false;
	}
	
	private void applyEvidence(int node, String outcomeId) {
		if (!outcomeId.equals(MISSING_VALUE)) {
			params.getNet().setEvidence(node, outcomeId);
		}
	}
	

	@Override
	protected void setup(Context context) throws IOException {
		params = new Parameters(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Network net = params.getNet();
		net.clearAllEvidence();
		String[] tokens = value.toString().split(" ");
		int[] evidenceNodes = params.getEvidenceNodes();
		for (int i = 0; i < evidenceNodes.length; i ++) {
			applyEvidence(evidenceNodes[i], tokens[i]);
		}
		
		boolean updatedBeliefs = false;
		
		
		int[][] families = params.getFamilies();
		int familyIndexBits = Parameters.bitCount(families.length);
		for (int familyIndex = 0; familyIndex < families.length; familyIndex ++) {
			int[] family = families[familyIndex];
			if (missingNodesExist(family)) {
				if (!updatedBeliefs) {
					net.updateBeliefs();
					updatedBeliefs = true;
				}

				ArrayList<Integer> missingNodes = new ArrayList<Integer>();
				findMissingNodes(family, missingNodes);
				int[] odoRange = new int[missingNodes.size()];
				for (int i = 0; i < odoRange.length; i ++) {
					int outcomeCount = net.getOutcomeCount(missingNodes.get(i));; 
					odoRange[i] = outcomeCount;
				}

				Odometer odometer = new Odometer(odoRange);
				
				int norm = 0;
				int totalSpinCount = odometer.getTotalSpinCount();
				for (int i = 0; i < odoRange.length; i ++) {
					norm += totalSpinCount / odoRange[i];
				}
				
				do {
					BitBuffer buf = new BitBuffer();
					buf.addBits(familyIndex, familyIndexBits);
					double p = 0;
					for (int i = 0; i < family.length; i ++) {
						int handle = family[i];
						
						int outcome;
						int pos = missingNodes.indexOf(handle);
						if (pos >= 0) {
							int[] odoValue = odometer.getValue();
							outcome = odoValue[pos];
							p += net.getNodeValue(handle)[outcome];
						} else {
							outcome = net.getEvidence(handle);
						}

						int nodeOutcomeBits = Parameters.bitCount(net.getOutcomeCount(handle));
						buf.addBits(outcome, nodeOutcomeBits);
					}
					p /= norm;
					context.write(new BytesWritable(buf.getOutput()), new DoubleWritable(p));
				} while (!odometer.next());
			} else {
				BitBuffer buf = new BitBuffer();
				buf.addBits(familyIndex, familyIndexBits);
				for (int i = 0; i < family.length; i ++) {
					int handle = family[i];
					int nodeOutcomeBits = Parameters.bitCount(net.getOutcomeCount(handle));
					buf.addBits(net.getEvidence(handle), nodeOutcomeBits);
				}
				context.write(new BytesWritable(buf.getOutput()), ONE);
			}
		}
	}
}
