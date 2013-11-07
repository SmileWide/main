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
import java.util.BitSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import smile.wide.utils.Pair;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	String record = new String();
	int maxAdjacency = 0;
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		maxAdjacency = conf.getInt("maxAdjacency", 0);
	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		record = value.toString();
		String[] values = record.split(",|\t| ");
		//HERE PARSE RECORD AND GENERATE ALL POSSIBLE COMBOS (WANT TO SEE HOW BAD IT GETS)
		ArrayList<Pair<Integer,String>> set= new ArrayList<Pair<Integer,String>>();
		powerset(context,set,values,0,maxAdjacency+2);
		//cleanup
	}
	
	/**Powerset generator
	 * 
	 * @param context used for outputting MapReduce Key-Value pair
	 * @param set current subset of powerset
	 * @param vals complete array
	 * @param vctr counter in array
	 * @param max max size of subset
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void powerset(Context context, ArrayList<Pair<Integer,String>> set, String[] vals, int vctr, int max) throws IOException, InterruptedException {
		String assignment = "";
		if(set.size()>0) {
			for(int x=0; x<set.size();++x) {
				if(assignment.length()>0) {
					assignment+="+v"+set.get(x).getFirst()+"="+set.get(x).getSecond();
				}
				else
					assignment="v"+set.get(x).getFirst()+"="+set.get(x).getSecond();
			}
			context.write(new Text(assignment), new IntWritable(1));
		}
		if(set.size()+1<=max) {
			for(int x=vctr;x<vals.length;++x) {
				set.add(new Pair<Integer,String>(x,vals[x]));
				powerset(context,set,vals,x+1,max);
				set.remove(set.size()-1);
			}
		}
	}
}
