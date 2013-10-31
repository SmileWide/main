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
package smile.wide;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/** The reducer merges two maps of maps in "the most natural way":
 * 
 * The resulting keySet is the union of the keySets. 
 * The corresponding values for each keys (which are maps) are merged as follows:
 * 
 * The resulting keySet is the union of the keySets.
 * Where key is in both maps, the result has the sum of the corresponding values. 
 * 
 * 
 * @author tomas.singliar@boeing.com
 *
 */
public class AttributeValueHistogramReducer 
	extends Reducer<Text, MapWritable, Text, MapWritable>
{
	
	// =================================================================
	// statics
	private static final Logger s_logger;
	
	static
	{
		s_logger = Logger.getLogger(AttributeValueHistogramReducer.class);
		s_logger.setLevel(Level.DEBUG);	
	}

	// =================================================================
	// reducer implementations
	
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
		throws IOException, InterruptedException
	{
		// Let's have a map and internally collect them
		
		int maps = 0;
		int vals = 0;
				
		HashMap<Text, Integer> myMap = new HashMap<Text, Integer>(); 
		
		for (MapWritable m : values)
		{		
			maps++;
			for (Writable valName : m.keySet())
			{
				
				Text val = (Text) valName;
				Integer count = ((IntWritable)(m.get(valName))).get();
				if (myMap.containsKey(val))
				{
					myMap.put(val, myMap.get(val) + count);
				}
				else
				{
					myMap.put(val, count);
					vals++;
				}				
			}
		}
		
		s_logger.debug("Reducer/combiner got " + maps + 
					   " maps, with a total of " + vals + 
					   " distinct values for attribute `" + key + "`"); 
		
		// now output
		// key is key 
		// value is myMap as MapWritable<Text, IntWritable>
		
		MapWritable output = new MapWritable();		
		for (Text t : myMap.keySet())
		{
			s_logger.debug("Outputting count " + myMap.get(t) + " for attribute " + t);
			output.put(t,new IntWritable(myMap.get(t)));
		}
				
		context.write(key, output);		
		
	}
}
