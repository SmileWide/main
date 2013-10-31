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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@Deprecated
public class InferenceJob {
		
	public static void main(String[] aaaarghs) throws URISyntaxException
	{
		if (aaaarghs.length != 2)
		{
			System.err.println("Usage: InferenceJob <infile> <outfile>");
			System.exit(1);
		}
		
		try
		{					
			JobConf conf = new JobConf();
			DistributedCache.createSymlink(conf);
			DistributedCache.addCacheArchive(
					new URI("hdfs://130.42.96.139:9000/jParInf/lib/linux64/smile.jar#smile.jar"), conf);
			DistributedCache.addCacheFile(
					new URI("hdfs://130.42.96.139:9000/jParInf/lib/linux64/libjsmile.so#libjsmile.so"), conf);					
											
									
			System.out.println("Create symlinks: " + DistributedCache.getSymlink(conf));
			
			Job job = new Job(conf);			
			job.setJarByClass(InferenceJob.class);
			job.setJobName("SMILE Inference test");
			
			FileInputFormat.addInputPath(job, new Path(aaaarghs[0]));
			FileOutputFormat.setOutputPath(job, new Path(aaaarghs[1]));
			
			job.setMapperClass(InferenceMapper.class);
			job.setReducerClass(InferenceReducer.class);
			
			// but redefine them for the mapper			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(ArrayWritable.class);
			
			// set both the map and reduce in/out classes
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			// but redefine them for the mapper			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(ArrayWritable.class);
											
			
			System.out.println("The job working directory is " + job.getWorkingDirectory());			
						
			System.exit(job.waitForCompletion(true) ? 3 : 0);
			
		}
		catch (IOException e)
		{
			System.err.println("Something went badly wrong in IO.");
			System.exit(2);
		} catch (InterruptedException e) {
			System.err.println("Job interrupted.");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("ClassNotFound exception.");
			e.printStackTrace();
		}
		
	}
	
}
