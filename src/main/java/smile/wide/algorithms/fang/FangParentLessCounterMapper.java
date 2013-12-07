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
package smile.wide.algorithms.fang;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class FangParentLessCounterMapper extends Mapper<LongWritable, Text, Text, VIntWritable> {
	String record = new String();
	VIntWritable one = new VIntWritable(1);
	int x = 0;
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		//get configuration
		Configuration conf = context.getConfiguration();
		//get variable under investigation
		x = conf.getInt("VarX", 0);
	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		record = value.toString();
		String[] values = record.split(",|\t| ");
        context.write(new Text("v"+x+"="+values[x]), one);
	}
}
