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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndMaxPValueMapper extends Mapper<LongWritable, Text, Text, Text> {
	String record = new String();
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		record = value.toString();
		String[] values = record.split("\t| ");
		String[] variables = values[0].split(",");
		//compare first 2 variables (x and y)
		Integer a = Integer.decode(variables[0].substring(1));
		Integer b = Integer.decode(variables[1].substring(1));
		//sort them
		//recreate key and value
		String mykey = "";
		if(b < a)
			mykey = variables[1] + "," + variables[0];
		else
			mykey = variables[0] + "," + variables[1];
		String myvalue = "";
		if(variables.length>2) {
			for(int x=2;x<variables.length;++x) {
				if(x>2)
					myvalue += "," + variables[x];
				else
					myvalue += variables[x];
			}
			myvalue += "\t";
		}
		myvalue += values[1];
		context.write(new Text(mykey), new Text(myvalue));
	}
	
}
