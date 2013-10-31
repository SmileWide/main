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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.wide.hadoop.io.DoubleArrayWritable;


@Deprecated
public class InferenceDriver 
	extends Configured implements Tool
{

	/* static {
		// pick up standard config
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	};*/
	
	private static final Logger s_logger;
	
	static
	{
		s_logger = Logger.getLogger(InferenceDriver.class);
		s_logger.setLevel(Level.DEBUG);
	}
	
	
	@Override
	public int run(String[] filteredargs) throws Exception {
		
		String inPath;
		String outPath;
		
		if (filteredargs.length != 2)
		{
			System.err.println("Usage: InferenceDriver <infile> <outfile>");
			
			String yousaid = "";
			for (String s : filteredargs)
			{				
				yousaid += s + " ";
			}
			System.err.println("You said to the driver: " + yousaid);
			System.err.println("Are those generic arguments supposed to be there?");
		}
		
		inPath = filteredargs[filteredargs.length-2];
		outPath = filteredargs[filteredargs.length-1];
		
		try
		{					
			Configuration conf = getConf();
			
			conf.set("keep.failed.task.files","true");			
			conf.set("keep.failed.task.pattern","*");
			
			
			/* we'll do this through "-libjars" now
			DistributedCache.createSymlink(conf);												
			DistributedCache.addCacheArchive(
					new URI("hdfs://130.42.96.139:9000/jParInf/lib/linux64/smile.jar#smile.jar"), conf);
			DistributedCache.addCacheFile(
					new URI("hdfs://130.42.96.139:9000/jParInf/lib/linux64/libjsmile.so#libjsmile.so"), conf);					
											
			
			System.out.println("Create symlinks: " + DistributedCache.getSymlink(conf)); 
			*/
			
			Job job = new Job(conf);			
			job.setJarByClass(InferenceJob.class);
			job.setJobName("SMILE Inference test");
			
			FileInputFormat.addInputPath(job, new Path(inPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			
			job.setMapperClass(InferenceMapper.class);
			job.setReducerClass(InferenceReducer.class);
			
			// set both the map and reduce in/out classes
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			// but redefine them for the mapper			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DoubleArrayWritable.class);					
			
			s_logger.info("Job working directory is " + job.getWorkingDirectory());			
						
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
		
		
		
		return 0;
	}
	
	public static void main(String args[]) throws Exception
	{			
		int exitcode = -1;
		
		// doesn't work - because the first argument passed didn't begin with a "-"
		// int exitcode = ToolRunner.run(new Configuration(), new InferenceDriver(), args);		
		
		// let's do it explicitly 
		{
			Configuration conf = new Configuration();	

			String yousaid = "";
			for (String s : args)
			{				
				yousaid += s + " ";
			}
			s_logger.debug("InferenceDriver started with these arguments: " + yousaid);				

			GenericOptionsParser parser = new GenericOptionsParser(conf, args);		
			String[] restOfArgs = parser.getRemainingArgs();

			yousaid = "";
			for (String s : restOfArgs)
			{				
				yousaid += s + " ";
			}
			s_logger.debug("After generic option parsing, the arguments are: " + yousaid);

			InferenceDriver d = new InferenceDriver();
			d.setConf(conf);
			exitcode = d.run(restOfArgs);
		}

		System.exit(exitcode);
	}
	

}
