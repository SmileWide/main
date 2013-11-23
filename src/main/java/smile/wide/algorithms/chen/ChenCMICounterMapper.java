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
package smile.wide.algorithms.chen;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class ChenCMICounterMapper extends Mapper<LongWritable, Text, Text, VIntWritable> {
	String record = new String();
	VIntWritable one = new VIntWritable(1);
	int x = 0;
	int y = 0;
	String[] sZ = {};
	Set<Integer> Z = new HashSet<Integer>();
	String value1 = new String();
	String value2 = new String();
	String value3 = new String();
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		x=conf.getInt("Vx", -1);
		y=conf.getInt("Vy", -1);
		sZ = conf.getStrings("Z");
	}
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		for(int i=0;i<sZ.length;++i)
			Z.add(Integer.decode(sZ[i]));
		record = value.toString();
		String[] values = record.split(",|\t| ");
		value1="v"+x+values[x];
		value2="v"+y+values[y];
		boolean first=true;
		value3="";
		for(Integer z : Z) {
			if(!first) {
				value3+="+v"+z+"="+values[z];
			}
			else {
				first = false;
				value3="v"+z+"="+values[z];
			}
		}
		context.write(new Text(value1+"+"+value3), one);
		context.write(new Text(value2+"+"+value3), one);
		context.write(new Text(value1+"+"+value2+"+"+value3), one);
		context.write(new Text(value3), one);
	}
}
