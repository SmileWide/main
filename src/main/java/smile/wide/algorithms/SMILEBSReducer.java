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
package smile.wide.algorithms;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import smile.wide.hadoop.io.StrucLogLikeWritable;

/**Reducer class used for running SMILE's Bayesian Search algorithm.
 * Takes the output from the Mappers (Network in String form, Structure Score)
 * Find Network with max score.
 * @author m.a.dejongh@gmail.com
 */
public class SMILEBSReducer extends Reducer<Text, StrucLogLikeWritable, Text, StrucLogLikeWritable> {
	@Override
	/** Reduce function, takes networks and Bayesian Scores and finds the network with the max score*/
	public void reduce(Text key, Iterable<StrucLogLikeWritable> values, Context context) throws IOException, InterruptedException {
		double total = Double.NEGATIVE_INFINITY;
		String MaxNet = "ThisIsNotRight";
		for (StrucLogLikeWritable p: values) {
			if(p.getLogLike() > total) {
				total = p.getLogLike();
				MaxNet = p.getNw();
			}
		}
		context.write(key, new StrucLogLikeWritable(MaxNet, total)); //might be sufficient to write just the net.
	}
}
