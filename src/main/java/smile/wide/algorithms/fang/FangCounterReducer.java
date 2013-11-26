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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**Reducer class
 * @author m.a.dejongh@gmail.com
 */
public class FangCounterReducer extends Reducer<Text, VIntWritable, Text, VIntWritable> {
	@Override
	/** Reduce function, for now should generate counts*/
	public void reduce(Text key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
		int total = 0;
		for (VIntWritable p: values) {
			total += p.get();
		}
		context.write(key, new VIntWritable(total));
	}
}
