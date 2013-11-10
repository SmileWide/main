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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

import smile.wide.utils.Pair;
import smile.wide.utils.SMILEMath;

/**Reducer class
 * @author m.a.dejongh@gmail.com
 */
public class HadoopIndCountProcReducer extends Reducer<Text, Text, Text, Text> {
	String mykey = new String();
	String temp = new String();
	double significance = 0.05;
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		significance = conf.getFloat("significance", (float) 0.05);
	}

	@Override
	/** Reduce function, for now should generate counts*/
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		mykey = key.toString();
		String[] variables = mykey.split(",");
		
		Map<List<String>,Integer> elements = new HashMap<List<String>,Integer>();
		Map<List<String>,Pair<Set<String>,Set<String>>> nonzeros = new HashMap<List<String>,Pair<Set<String>,Set<String>>>();
		Map<List<String>,Pair<Integer,Integer>> emptyrc = new HashMap<List<String>,Pair<Integer,Integer>>();
		Map<List<String>,Integer> adder = new HashMap<List<String>,Integer>();
		List<Set<String>> states = new ArrayList<Set<String>>();
		ArrayList<Integer> cardinalities = new ArrayList<Integer>();
		for(int x=0;x<variables.length;++x) {
			states.add(new HashSet<String>());
			cardinalities.add(0);
		}
		for (Text p: values) {
			//decode from string
			temp = p.toString();
			String[] pair = temp.split("=");
			Integer count = Integer.decode(pair[1]);
			String[] myvalues = pair[0].split(",");
			//calculate marginals
			List<String> xijk = new ArrayList<String>();
			List<String> x_jk = new ArrayList<String>();
			List<String> xi_k = new ArrayList<String>();
			List<String> x__k = new ArrayList<String>();
			for(int v=0;v<myvalues.length;++v) {
				//construct vector from values
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
				//collect node states
				states.get(v).add(myvalues[v]);
			}
			//record nonzero rows and columns
			if(nonzeros.get(x__k)==null)
				nonzeros.put(x__k, new Pair<Set<String>,Set<String>>(new HashSet<String>(),new HashSet<String>()));
			nonzeros.get(x__k).getFirst().add(myvalues[0]);
			nonzeros.get(x__k).getSecond().add(myvalues[1]);
			//record elements and counts
			elements.put(xijk, count);
			//aggregate row and column counts
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
		//calculate node cardinalities
		for(int x=0;x<variables.length;++x) {
			cardinalities.set(x, states.get(x).size());
		}
		//calculate empty rows/columns in contingency tables
		for(Entry<List<String>,Pair<Set<String>,Set<String>>> z : nonzeros.entrySet()) {
			emptyrc.put(z.getKey(), new Pair<Integer,Integer>(cardinalities.get(0)-z.getValue().getFirst().size(),cardinalities.get(1)-z.getValue().getSecond().size()));
		}
		//calculate degrees of freedom
    	// calc dof for the "undamaged" part
        int dof=1;
        int xandy=1;
        int condset=1;
        
        for (int i = 0; i <  (int) cardinalities.size(); i++)
        {
            if (i <2)
                xandy *= cardinalities.get(i) - 1;
            else
                condset *= cardinalities.get(i);
        }
    	dof = xandy * (condset - emptyrc.size());
    	// calc remainder
    	Iterator<Entry<List<String>, Pair<Integer, Integer>>> bitt;
    	bitt = emptyrc.entrySet().iterator();
    	while(bitt.hasNext())
    	{
    		Entry<List<String>, Pair<Integer, Integer>> bit = (Entry<List<String>, Pair<Integer, Integer>>) bitt.next();
    		int broken = 	(cardinalities.get(0) - 1 - ( ( (Pair<Integer, Integer>) bit.getValue()).getFirst() ) ) 
    						* 
    						(cardinalities.get(1) - 1 - ( ( (Pair<Integer, Integer>) bit.getValue()).getSecond() ) );
    		if((cardinalities.get(0)-1-(((Pair<Integer, Integer>) bit.getValue()).getFirst())) >= 0)
    			dof += broken;
    	}
        if (dof <= 0)
            dof = 1;
		//calculate g2 statistic
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
		g2*=2;
    	double pvalue = (double) SMILEMath.gammq((double) (0.5 * dof), (double) (0.5 * g2));
    	//String outcome = "dof " + dof + ", g2 " + g2 + ", pvalue " + pvalue;
    	String outcome = Double.toString(pvalue);
//    	if(pvalue > significance)
//    		outcome += " INDEPENDENT";
    	context.write(key, new Text(outcome));
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
