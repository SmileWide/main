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
import java.util.BitSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndependenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
		String[] values = record.split(",|\\t| ");
		//HERE PARSE RECORD AND GENERATE ALL POSSIBLE COMBOS (WANT TO SEE HOW BAD IT GETS)
		BitSet b = new BitSet(values.length+1);
		b.clear();
		int setsize = 0;
		String assignment = new String("");
		while(!b.get(values.length)) {
			assignment = "";
			if(setsize <= maxAdjacency+2) {
				for(int x=0; x<values.length;++x) {
					if(b.get(x)) {
						if(assignment.length()>0) {
							assignment+="+v"+x+"="+values[x];
						}
						else
							assignment="v"+x+"="+values[x];
					}
				}
				if(setsize > 0)
					context.write(new Text(assignment), new IntWritable(1));							
			}
			//increment b
	        for(int i = 0; i < b.size(); i++) {
	            if(!b.get(i)) {
	                b.set(i);
	                setsize=setsize+1;
	                break;
	            } else {
	                b.clear(i);
	                setsize=setsize-1;
	            }
	        }					
		}
		//cleanup
	}
}
