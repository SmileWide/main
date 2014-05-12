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
package smile.wide.algorithms.pc;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import smile.wide.hadoop.io.ItestPairArrayWritable;
import smile.wide.hadoop.io.ItestPairWritable;
import smile.wide.utils.Pair;

/**Reducer class
 * @author m.a.dejongh@gmail.com
 */
public class IndependenceTestReducer extends Reducer<Text, ItestPairArrayWritable, Text, ItestPairArrayWritable> {
	int nvar = 0;
	ArrayList<ArrayList<ArrayList<Integer>>> sepsets = new ArrayList<ArrayList<ArrayList<Integer>>>();

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		nvar = conf.getInt("nvar", 0);
        for (int x=0; x< nvar; x++) {
        	sepsets.add(new ArrayList<ArrayList<Integer> >());
        }
        for (int x=0; x< nvar; x++) {
        	for(int y=0; y < nvar; y++) {
            	sepsets.get(x).add(null);
        		
        	}
        }
	}

	@Override
	public void reduce(Text key, Iterable<ItestPairArrayWritable> values, Context context) throws IOException, InterruptedException {
		//we extract all results from the input
		//we remove any duplicate removed edges (keeping those with the smallest separator sets
		
		ArrayList<Integer> sepset = null;
		for (ItestPairArrayWritable p: values) {
			Writable[] a = p.get();
			ItestPairWritable l;
			for(int x = 0 ;x < a.length;++x) {
				l = (ItestPairWritable) a[x];
				int from = l.getFirst().getFirst();
				int to = l.getFirst().getSecond();
				Writable[] b = l.getSecond().get();
				sepset = new ArrayList<Integer>();
				for(int y=0;y<b.length;++y) {
					IntWritable i = (IntWritable) b[y];
					sepset.add(i.get());
				}
				if(sepsets.get(from).get(to) == null) {
					sepsets.get(from).set(to,sepset);
				} else {
					if(sepset.size() < sepsets.get(from).get(to).size() )
						sepsets.get(from).set(to, sepset);
				}
			}
		}
		//reconstruct result format and output
		ArrayList<Pair<Pair<Integer, Integer>, ArrayList<Integer>>> t = new ArrayList<Pair<Pair<Integer, Integer>, ArrayList<Integer>>>();
		for(int from = 0; from < sepsets.size(); ++from) {
			for(int to = from + 1; to < sepsets.size(); ++to) {
				if(sepsets.get(from).get(to) != null) {
					Pair<Integer, Integer> edge = new Pair<Integer, Integer>(from,to);
					Pair<Pair<Integer, Integer>, ArrayList<Integer>> element = new Pair<Pair<Integer, Integer>, ArrayList<Integer>>(edge,sepsets.get(from).get(to));
					t.add(element);
				}
			}
		}
		ItestPairArrayWritable result = new ItestPairArrayWritable(t);
		context.write(key, result);
	}
}
