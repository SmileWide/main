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
package smile.wide.algorithms.pc;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/** Job class to run PC's Independence tests on a hadoop cluster
 *  Sets up Hadoop configuration
 *  Distributes temp data file
 *  Starts MapReduce job
 *  Retrieves output data file
 * @author m.a.dejongh@gmail.com
 */
public class HadoopIndependenceJob extends Configured implements Tool {
	//parameters
	
	//paths for Input Data (txt file to be read into SMILE)
	//Stole this from Tomas for now. We need something better
	//placeholder for better idea
	//We need a more standardized approach
	/** Path to data storage */
	private String dataHDFSPath_ = "/user/mdejongh/data/"; 

	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
	@Override
	public int run(String[] params) throws Exception {
		Configuration conf = super.getConf();

		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Independence Test");
		job.setJarByClass(HadoopIndependenceJob.class);
		job.setMapperClass(HadoopIndependenceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(HadoopIndependenceReducer.class);
		job.setInputFormatClass(TextInputFormat.class);

		//Set input and output paths
		Path inputPath = new Path(conf.get("datainput"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("dataoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);
		return 0;
	}

	/** Returns basename of file*/
	private String basename(String fName) {		
		return fName.substring(fName.lastIndexOf('/')+1); 
	}
	
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new HadoopIndependenceJob(), args);
		System.exit(exitCode);
	}
}
