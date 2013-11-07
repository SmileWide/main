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
import java.util.Map;

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
		Map<String,ArrayList<String>> varvalues = new HashMap<String,ArrayList<String>>();
		for (Text p: values) {
			temp = p.toString();
			String[] pair = temp.split("=");
			String[] myvalues = pair[0].split(",");
			for(int x=0;x<myvalues.length;++x) {
				if(!varvalues.containsKey(variables[x]))
					varvalues.put(variables[x], new ArrayList<String>());
				varvalues.get(variables[x]).add(myvalues[x]);
			}
		}
		temp = "";
		for (String p : variables) {
			temp += varvalues.get(p).size() + ",";
		}
		context.write(key, new Text(temp));
	}
}
