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
import org.apache.mahout.common.IntPairWritable;

import smile.learning.DataSet;
import smile.wide.data.SMILEData;
import smile.wide.hadoop.io.ItestPairArrayWritable;
import smile.wide.utils.Pair;
import smile.wide.utils.Pattern;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class IndependenceTestMapper extends Mapper<LongWritable, Void, Text, ItestPairArrayWritable> {
	Pattern pat = new Pattern();	
	String datafile = "";
	SMILEData ds = new SMILEData();
	int adjacency = 0;
	int randSeed = 0;
	double significance = 0.05;
	int numberoftests = 0;
	boolean disc = true;
	ArrayList<ArrayList<Set<Integer>>> sepsets = new ArrayList<ArrayList<Set<Integer>>>(); 
	RandomIndependenceStep itest = null;
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		adjacency = conf.getInt("maxAdjacency", 0);
		significance = conf.getDouble("significance", 0.05);
		numberoftests = conf.getInt("numberoftests",0);
		disc = conf.getBoolean("disc", true);
		pat = new Pattern(conf.get("pattern"));
		datafile = conf.get("thedata");
		ds.Read(datafile);
		int nvar = pat.getSize();
		//initialize sepsets data structure further
        for (int x=0; x< nvar; x++) {
        	sepsets.add(new ArrayList<Set<Integer> >());
        }
        for (int i = 0; i < nvar; i++)
        {
            for (int x=0; x< nvar; x++) {
            	sepsets.get(i).add(new HashSet<Integer>());
            }
        }

	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Void value, Context context) throws IOException, InterruptedException  {
		//get seeds
		long k = key.get();
		randSeed = (int)(k & 0xffffffffL);
		ArrayList<Pair<Integer,Integer>> removed = new ArrayList<Pair<Integer,Integer>>();
		itest = new RandomIndependenceStep(randSeed,numberoftests);
		itest.removed = removed;
		try {
			itest.execute(ds, pat, disc, adjacency, significance, sepsets);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//process and output results
		ArrayList<Pair<Pair<Integer,Integer>,ArrayList<Integer>>> tresult = new ArrayList<Pair<Pair<Integer,Integer>,ArrayList<Integer>>>();
		for(Pair<Integer,Integer> i : removed) {
			int x = i.getFirst();
			int y = i.getSecond();
			ArrayList<Integer> l = new ArrayList<Integer>(sepsets.get(x).get(y));
			tresult.add(new Pair<Pair<Integer,Integer>,ArrayList<Integer>>(i,l));
		}
		ItestPairArrayWritable result = new ItestPairArrayWritable(tresult);
		context.write(new Text("key"), result);
	}
}
