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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import smile.Network;

public class Parameters {
	public static int bitCount(long x) {
		return Long.SIZE - Long.numberOfLeadingZeros(x);
	}


	public Network getNet() {
		return net;
	}

	public int[] getEvidenceNodes() {
		return evidenceNodes;
	}

	public int[][] getFamilies() {
		return families;
	}

	private Network net;
	private int[] evidenceNodes;
	private int[][] families;

	public static final String CONFKEY_DATA_ORDER = "dataorder";
	public static final String CONFKEY_FAMILIES = "families";

	private int[] idsToHandles(String allNodeIds) {
		String[] nodeIds = allNodeIds.split(":");
		int[] out = new int[nodeIds.length];
		for (int i = 0; i < nodeIds.length; i ++) {
			out[i] = net.getNode(nodeIds[i]);
		}
		return out;
	}
	
	public Parameters(Configuration conf) throws IOException {
		Path[] tempFiles = DistributedCache.getLocalCacheFiles(conf);
		net = new Network();
		net.readFile(tempFiles[0].toString());
		net.clearAllTargets();
		evidenceNodes = idsToHandles(conf.get(CONFKEY_DATA_ORDER));
		String [] allFamilies = conf.get(CONFKEY_FAMILIES).split("\t");
		families = new int[allFamilies.length][];
		for (int i = 0; i < allFamilies.length; i ++) {
			families[i] = idsToHandles(allFamilies[i]);
		}
	}
}
