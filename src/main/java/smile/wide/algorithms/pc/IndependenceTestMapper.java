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

import smile.learning.DataSet;
import smile.wide.data.SMILEData;
import smile.wide.utils.Pair;
import smile.wide.utils.Pattern;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class IndependenceTestMapper extends Mapper<LongWritable, Void, Text, Text> {
	Pattern pat = new Pattern();	
	String datafile = "";
	SMILEData ds = new SMILEData();
	int maxAdjacency = 0;
	int randSeed = 0;
	double significance = 0.05;
	int numberoftests = 0;
	ArrayList<ArrayList<Set<Integer>>> sepsets = new ArrayList<ArrayList<Set<Integer>>>(); 
	IndependenceStep itest = null;
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		maxAdjacency = conf.getInt("maxAdjacency", 0);
		significance = conf.getDouble("significance", 0.05);
		numberoftests = conf.getInt("numberoftests",0);
		pat = new Pattern(conf.get("pattern"));
		datafile = conf.get("thedata");
		ds.Read(datafile);
	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Void value, Context context) throws IOException, InterruptedException  {
		//get seeds
		long k = key.get();
		randSeed = (int)(k & 0xffffffffL);
		itest = new RandomIndependenceStep(randSeed,numberoftests);
		try {
			itest.execute(ds, pat, true, maxAdjacency, significance, sepsets);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//our output is the modified pattern and the new sepsets??
	}
	
}
