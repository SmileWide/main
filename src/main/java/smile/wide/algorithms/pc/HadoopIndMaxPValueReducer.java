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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**Reducer class
 * @author m.a.dejongh@gmail.com
 */
public class HadoopIndMaxPValueReducer extends Reducer<Text, Text, Text, Text> {
	String record = "";
	@Override
	/** Reduce function finds max pvalue for a test*/
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double max = -1;
		String sepset = "";
		for (Text p: values) {
			double pvalue = 0;
			record = p.toString();
			String[] parsed = record.split("\t");
			pvalue = Double.parseDouble(parsed[parsed.length-1]);
			if(pvalue > max) {
				max = pvalue;
				if(parsed.length > 1)
					sepset = "{" + parsed[0] + "}";
				else
					sepset = "{}";
			}
		}
		context.write(key, new Text(sepset));
	}
}
