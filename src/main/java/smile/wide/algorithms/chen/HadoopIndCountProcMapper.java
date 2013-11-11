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
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndCountProcMapper extends Mapper<LongWritable, Text, Text, Text> {
	String record = new String();
	String mykey = new String();
	String myvalue = new String();
	
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
		mykey = "";
		for(int x=0;x<variables.size();++x) {
			if(x!=0)
				mykey += "," + variables.get(x);
			else
				mykey += variables.get(x);
		}
		myvalue = "";
		for(int x=0;x<values.size();++x) {
			if(x!=0)
				myvalue += "," + values.get(x);
			else
				myvalue += values.get(x);
		}
		myvalue += "="+pair[1];
		context.write(new Text(mykey), new Text(myvalue));
	}
}
