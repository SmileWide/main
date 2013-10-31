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
package smile.wide.obsolete;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;

import smile.wide.hadoop.io.DoubleArrayWritable;


@Deprecated
public class InferenceReducer 
	extends Reducer<IntWritable, DoubleArrayWritable, Text, DoubleWritable>
{
	int _exampleCount = 0;
	double _totalClass0 = 0.0;
	double _totalClass1 = 0.0;
	
	// this class simply spits the posterior back to the output
	
	// could it accumulate the statistics in an instance variable?
	// How do we get it out then?
	
	// implements JobConfigurable
	public void configure(JobConf job) throws IOException {
        // Get the cached archives/files
        DistributedCache.getLocalCacheArchives(job);
        DistributedCache.getLocalCacheFiles(job);
    }
	
	@Override
	public void reduce(IntWritable id, Iterable<DoubleArrayWritable> values, Context context)
		throws IOException, InterruptedException
	{
		// just try to count up, for experiment's sake,
		// and spit out the output
		_exampleCount++;
		
		for (DoubleArrayWritable post : values)
		{			
			Writable zerocount = post.get()[0];
			Writable onecount =  post.get()[1];
			_totalClass0 += ((DoubleWritable)zerocount).get();
			_totalClass1 += ((DoubleWritable)onecount).get();
			// _totalClass1 += ((DoubleWritable[])(post.get()))[1].get();
		}
		
		context.write(new Text("Class 0\n"), new DoubleWritable(_totalClass0));
		context.write(new Text("Class 1\n"), new DoubleWritable(_totalClass1));
	}
	
}
