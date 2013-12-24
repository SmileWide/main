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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import smile.wide.utils.Pair;
import smile.wide.utils.Pattern;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndCounterMapper extends Mapper<LongWritable, Text, Text, VIntWritable> {
	String record = new String();
	Pattern pat = new Pattern();
	int maxAdjacency = 0;
	VIntWritable one = new VIntWritable(1);
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		maxAdjacency = conf.getInt("maxAdjacency", 0);
		pat = new Pattern(conf.get("pattern"));
	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		record = value.toString();
		String[] values = record.split(",|\t| ");
		setGenerator(context, values, maxAdjacency);
	}
	
	void depthFirstSearch(Set<Integer> set, Set<Integer> marked, int current) {
		if( !marked.contains(current) ){
			marked.add(current);
			for(Integer i : set) {
				if(pat.getEdge(current, i) != Pattern.EdgeType.None) {
					depthFirstSearch(set,marked,i);
				}
			}
		}
	}
	
	void setGenerator(Context context, String[] vals, int adjacency) throws IOException, InterruptedException {
		for(int x=0;x<vals.length;++x) {
			for(int y=0;y<vals.length;++y) {
				if(x!=y && pat.getEdge(x,y) != Pattern.EdgeType.None) {
					String assignment = "v"+x+"="+vals[x]+"+v"+y+"="+vals[y];
					if(adjacency > 0)
						sepsetFunction(context,x,y,adjacency,vals,assignment);
					else if(x<y)
						context.write(new Text(assignment), one);
				}
			}
		}
	}
	
	void sepsetFunction(Context context, int x, int y, int card, String[] vals, String assignment) throws IOException, InterruptedException {
		int nvar = vals.length;
        // populate elements vector
        ArrayList<Integer> elements=new ArrayList<Integer>();
        int i;
        for (i = 0; i < nvar; i++) {
            if (i != x && i != y && pat.getEdge(x, i) == Pattern.EdgeType.Undirected) {
                    elements.add(i);
            }
        }
        // check for enough elements
        if ((int) elements.size() < card) {
            return;
        }
        // generate conditioning sets
        ArrayList<Boolean> binvec = new ArrayList<Boolean>(Collections.nCopies(elements.size(), false));
        for (i = 0; i < card; i++) {
            binvec.set(i,true);
        }
        // test for conditional independence
        boolean first = true;
    	MutableInt cur = new MutableInt(card - 1);
    	MutableInt ones = new MutableInt(0);
        for (; first || NxtSubset(binvec, cur, ones); ) {
            first = false;
			String sepset = "";
            for (i = 0; i < (int) binvec.size(); i++) {
                if (binvec.get(i)) {
                    int z = elements.get(i);
        			sepset += "+v"+z+"="+vals[z];
                }
            }
			context.write(new Text(assignment+sepset), one);//FIXED BUG, SHOULD HAVE SIGNIFICANTLY LESS KVPs
        }
	}
	
    private boolean NxtSubset(ArrayList<Boolean> binvec, MutableInt cur, MutableInt ones) {
    	if (cur.intValue() < 0)	{
    		return false;
    	}
    	int size = binvec.size();
    	if ((cur.intValue() + 1 < size) && !binvec.get(cur.intValue() + 1))	{
    		binvec.set(cur.intValue(),false);
    		cur.increment();
    		binvec.set(cur.intValue(),true);
    		return true;
    	}
    	else {
    		ones.increment();
    		for (int i = cur.intValue() - 1; i >= 0; i--) {
    			if (binvec.get(i)) {
    				if (!binvec.get(i + 1))	{
    					binvec.set(i,false);
    					binvec.set(i + 1,true);
    					int j;
    					for (j = i + 2; j < (i + ones.intValue() + 2); j++)	{
    						binvec.set(j,true);
    					}
    					cur.setValue(j - 1);
    					ones.setValue(0);
    					for (; j < size; j++) {
    						binvec.set(j,false);
    					}
    					return true;
    				}
    				else {
    					ones.increment();
    				}
    			}
    		}
    		return false;
    	}
    }
}
