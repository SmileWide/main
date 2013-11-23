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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class ChenMICounterMapper extends Mapper<LongWritable, Text, Text, VIntWritable> {
	String record = new String();
	String assignment = new String();
	VIntWritable one = new VIntWritable(1);
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		record = value.toString();
		String[] values = record.split(",|\t| ");
		for(int i = 0; i < values.length ; ++i) {
			assignment="v"+i+"="+values[i];
			context.write(new Text(assignment), one);
			for(int j=i+1; j < values.length ; ++j) {
				assignment="v"+i+"="+values[i]+"+v"+j+"="+values[j];
				context.write(new Text(assignment), one);
			}
		}
	}
}
