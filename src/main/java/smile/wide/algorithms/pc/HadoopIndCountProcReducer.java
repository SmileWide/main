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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableInt;
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
		
		Map<List<String>,Integer> elements = new HashMap<List<String>,Integer>();
		Map<List<String>,Integer> adder = new HashMap<List<String>,Integer>();
		for (Text p: values) {
			temp = p.toString();
			String[] pair = temp.split("=");
			Integer count = Integer.decode(pair[1]);
			String[] myvalues = pair[0].split(",");
			List<String> xijk = new ArrayList<String>();
			List<String> x_jk = new ArrayList<String>();
			List<String> xi_k = new ArrayList<String>();
			List<String> x__k = new ArrayList<String>();
			for(int v=0;v<myvalues.length;++v) {
				xijk.add(myvalues[v]);
				if(v==0)
					x_jk.add(null);
				else
					x_jk.add(myvalues[v]);
				if(v==1)
					xi_k.add(null);
				else
					xi_k.add(myvalues[v]);
				if(v<2)
					x__k.add(null);
				else
					x__k.add(myvalues[v]);
			}
			elements.put(xijk, count);
			if(adder.get(x_jk)!=null)
				adder.put(x_jk, adder.get(x_jk)+count);
			else
				adder.put(x_jk, count);
			if(adder.get(xi_k)!=null)
				adder.put(xi_k, adder.get(xi_k)+count);
			else
				adder.put(xi_k, count);
			if(adder.get(x__k)!=null)
				adder.put(x__k, adder.get(x__k)+count);
			else
				adder.put(x__k, count);
		}
		double g2 = 0.0;
		for(Entry<List<String>,Integer> q : elements.entrySet()) {
			double xijk = q.getValue();
			ArrayList<String> marginal = new ArrayList<String>();
			for(String z : q.getKey())
				marginal.add(new String(z));
			marginal.set(0, null);
			double x_jk = adder.get(marginal);
			marginal.set(0,q.getKey().get(0));
			marginal.set(1,null);
			double xi_k = adder.get(marginal);
			marginal.set(0, null);
			double x__k = adder.get(marginal);
			double logexijk = Math.log(x_jk)+Math.log(xi_k)-Math.log(x__k);
			g2 += xijk*(Math.log(xijk)-logexijk);
		}
		
		context.write(key, new Text(Double.toString(g2)));
		/*
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
		/*
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
		/*
		temp = "";
		for (int x = 0; x<variables.length;++x) {
			temp += varvalues.get(x).size() + ",";
		}
		context.write(key, new Text(temp));
		*/
	}
}
