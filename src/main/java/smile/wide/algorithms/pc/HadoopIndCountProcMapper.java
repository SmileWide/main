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

import smile.wide.utils.Pattern;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndCountProcMapper extends Mapper<LongWritable, Text, Text, Text> {
	String record = new String();
	String mykey = new String();
	String myvalue = new String();
	Pattern pat = new Pattern();
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		pat = new Pattern(conf.get("pattern"));
	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		record = value.toString();
		String[]  pair= record.split("\t| ");
		String[] assignments = pair[0].split("\\+");
		ArrayList<String> variables = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		for(int x=0;x<assignments.length;++x) {
			String[] assignment = assignments[x].split("=");
			variables.add(assignment[0]);
			values.add(assignment[1]);
		}
		/*from the created counts we generate all possible combinations of
		* (x,y,Z), ((x,y,Z),count)
		*/
		for(int x=0;x<variables.size();++x) {
			int xx = Integer.decode(variables.get(x).replace("v", ""));
			for(int y=0;y<variables.size();++y) {
				int yy = Integer.decode(variables.get(y).replace("v", ""));
				if(x!=y && pat.getEdge(xx, yy) != Pattern.EdgeType.None) {//check if x and y are connected?
					mykey = variables.get(x) + "," + variables.get(y);
					myvalue = values.get(x) + "," + values.get(y);
					boolean all_connected = true;
					if(variables.size()>2) {
						//check if all variables in Z are connected to x
						for(int z=0;z<variables.size();++z) {
							if(z!=x && z!=y) {
								int zz=Integer.decode(variables.get(z).replace("v", ""));;
								if(pat.getEdge(xx, zz) == Pattern.EdgeType.None) {
									all_connected = false;
									break;
								}
								mykey += "," + variables.get(z);
								myvalue += "," + values.get(z);
							}
						}
					}
					myvalue+= "=" + pair[1];
					if(all_connected)
						context.write(new Text(mykey), new Text(myvalue));
				}
			}
		}
	}
}
