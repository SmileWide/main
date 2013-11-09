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
package smile.wide.algorithms.fang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**Reducer class
 * @author m.a.dejongh@gmail.com
 */
public class HadoopIndCountProcReducer extends Reducer<Text, Text, Text, Text> {
	String mykey = new String();
	String temp = new String();
	@Override
	/** Reduce function, for now should generate counts*/
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		mykey = key.toString();
		String[] variables = mykey.split(",");
		ArrayList<HashSet<String>> varvalues = new ArrayList<HashSet<String>>();
		for(int x=0;x<variables.length;++x)
			varvalues.add(new HashSet<String>());
		ArrayList<Integer> counts = new ArrayList<Integer>();
		for (Text p: values) {
			temp = p.toString();
			String[] pair = temp.split("=");
			counts.add(Integer.getInteger(pair[1]));
			String[] myvalues = pair[0].split(",");
			for(int x=0;x<myvalues.length;++x) {
				varvalues.get(x).add(myvalues[x]);
			}
		}
		/*At this point we have for all variables their values (and thus their cardinality)
		* and we have all counts in an array
		* We have Xijk, we need X_jk , Xi_k, X__k
		* currently we get all counts for this pair of variables once, so it seems to be necessary to do all calculations with them
		* so the roles of x, y, and Z have to be changed.
		* We need to know the size of Z
		* Max size of Z is probably going to be 8, so in total we're looking at 10 variables divided into 3 groups, i.e. x, y, Z
		* special case is Z = 0. Then it's just pair wise independence.
		* should be O(n^2), pick x, pick y, rest is Z
		* 
		*/
		for(int x=0;x<variables.length;++x) {
			for(int y=0;y<variables.length;++y) {
				if(y!=x) {
					ArrayList<String> Z = new ArrayList<String>();
					for(int z=0;z<variables.length;++z) {
						if(z!=x && z!= y)
							Z.add(variables[z]);
					}
					//do something here.
				}
			}
		}
		
		/*Somehow build a tree structure with the counts like an ADTREE??
		 * we have a sorted list of variables
		 */
		
		temp = "";
		for (int x = 0; x<variables.length;++x) {
			temp += varvalues.get(x).size() + ",";
		}
		context.write(key, new Text(temp));
	}
}
