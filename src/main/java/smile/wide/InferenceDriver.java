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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.wide.hadoop.io.DoubleArrayWritable;



/** The class used by Network.infer(...) to start the back-end Hadoop inference job.
 * 
 */
public class InferenceDriver 
	extends Configured implements Tool
{

	// =========================================================================================
	// Logging 
	private static final Logger s_logger;

	static
	{
		s_logger = Logger.getLogger(InferenceDriver.class);
		s_logger.setLevel(Level.DEBUG);
	}

	// =========================================================================================
	// Paths, locations, etc 
  
	private String libHDFSPath_ = "/user/tsingliar/lib/linux64";
	
	private String inPath_ = null;
	private String outPath_ = null;	
		
	// file that contains the Bayesian network which will be processed by the inference phase
	private String networkFileHDFSPath_ = null; 	
	private Configuration conf_ = getConf();	
	
	
	// =========================================================================================
	// run
		
	/** Main per-instance inference driver.
	 *  Takes 2 arguments "on the command line" - input and output path. 
	 * 
	 * Relevant configuration parameters:
	 * - xdata.bayesnets.smile.library.path
	 * - xdata.bayesnets.networkfile
	 * - xdata.bayesnets.datasetreader.class
	 * - xdata.bayesnets.datasetreader.filter
	 * - xdata.bayesnets.datasetreader.variablenames
	 * - xdata.bayesnets.datasetreader.instid
	 * - xdata.bayesnets.queryvariable
	 * 
	 * TODO: update the driver to answer a set of queries, not just one
	 * 
	 */
	@Override
	public int run(String[] filteredargs) throws Exception {
		
		try
		{							
			// retrieve the input and output paths
			if (filteredargs.length != 2)
			{
				System.err.println("Usage: InferenceDriver <input-path> <output-dir>");
				return -1;
			}
			inPath_ = filteredargs[filteredargs.length-2];
			outPath_ = filteredargs[filteredargs.length-1];
			
			// locate the native libraries
			Configuration conf = getConf();
			
			String configuredLibHDFSPath_ = conf_.get("xdata.bayesnets.smile.library.path");
			if (configuredLibHDFSPath_ == null || configuredLibHDFSPath_.isEmpty())
			{
				s_logger.warn("SMILE library path defaulting to " + libHDFSPath_);
				s_logger.warn("Set xdata.bayesnets.smile.library.path to change. ");
			}
			else
			{
				libHDFSPath_ = configuredLibHDFSPath_;
			}
						
			// put the libraries in the job working dir
			DistributedCache.createSymlink(conf_);		
			try {			
				DistributedCache.addCacheFile(					
						new URI(libHDFSPath_ + "/smile.jar#smile.jar"), conf_);			
				DistributedCache.addCacheFile(
						new URI(libHDFSPath_ + "/libjsmile.so#libjsmile.so"), conf_);
				DistributedCache.addCacheFile(
						new URI(networkFileHDFSPath_ + "#"  + basename(networkFileHDFSPath_)), conf_);
			} catch (URISyntaxException e) {
				s_logger.fatal("Bad URL for network file.");
				return -12;				
			}
			
			// assume the network and data is configured already - for example
			/*
			conf_.set("xdata.bayesnets.networkfile", basename(modifiedNetwork_));			
			conf_.set("xdata.bayesnets.datasetreader.class", FacebookCSVReader.class.getName());
			conf_.set("xdata.bayesnets.datasetreader.filter", "3,5,7,10,11,12" );
			conf_.set("xdata.bayesnets.datasetreader.variablenames", 
					"FirstName,MiddleName,Sex,IsAppUser,LikesCount,FriendCount" );
			conf_.set("xdata.bayesnets.datasetreader.instid", "1");
			conf_.set("xdata.bayesnets.queryvariable", "Age");			
			*/
			
					
			Job job = new Job(conf);			
			job.setJarByClass(InferenceDriver.class);
			job.setJobName("SMILE-WIDE Inference");
			
			FileInputFormat.addInputPath(job, new Path(inPath_));
			FileOutputFormat.setOutputPath(job, new Path(outPath_));
			
			job.setMapperClass(PerInstanceInferenceMapper.class);						
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
		s_logger.error("Not intended to be run directly - called from Network.java");		
	}
	
	// =====================================================================
	// Little Helpers
	
	private String basename(String fName) {		
		return fName.substring(fName.lastIndexOf('/')+1); 
	}
	

}


/*

try	
{	
				
	// the principle for whether to use string column names or integer column indexes:
	// - when talking about variables in the BN, use string names
	// - when talking about data munging, use column indexes.
	
	
	Job job = new Job(conf_);
	
	job.setJarByClass(ExperimentDriver.class);	// use this jar
	job.setJobName("Facebook Inference Performance Test");
	
	FileInputFormat.addInputPath(job, new Path(inPath_));
	FileOutputFormat.setOutputPath(job, new Path(outPath_));
	
	job.setMapperClass(PerInstanceInferenceMapper.class);
	// there need not be any reducer
	// job.setReducerClass(PerInstanceInferenceReducer.class);
	
	// set both the map and reduce in/out classes
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	// but redefine them for the mapper		
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(DoubleArrayWritable.class);					
					
	s_logger.info("Job working directory is " + job.getWorkingDirectory());			
				
	return job.waitForCompletion(true) ? 0 : 1;
	
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

return 2;

*/