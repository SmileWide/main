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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
	@Override
	public int run(String[] params) throws Exception {
		Configuration conf = super.getConf();

		//GAME PLAN:
		//1. we need code for count calculation (is there but needs tweaking)
		calculateCounts(conf);//we can skip since we've already calculated this
		//2. we need code for count processing into p-values (needs more testing)
		processCounts(conf);
		//3. we need code for getting max p-value for a test
		extractMaxPValue(conf);
		return 0;
	}

	void calculateCounts(Configuration conf) throws Exception {
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Independence Test - Calculate Counts");
		job.setJarByClass(HadoopIndependenceJob.class);
		job.setMapperClass(HadoopIndCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(HadoopIndCounterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(240);

		//Set input and output paths
		Path inputPath = new Path(conf.get("datainput"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("countoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);	
	}
	
	void processCounts(Configuration conf) throws Exception {
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Independence Test - Process Counts");
		job.setJarByClass(HadoopIndependenceJob.class);
		job.setMapperClass(HadoopIndCountProcMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(HadoopIndCountProcReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(240);

		//Set input and output paths
		Path inputPath = new Path(conf.get("countoutput"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("processedcounts"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);	
	}

	void extractMaxPValue(Configuration conf) throws Exception{
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Independence Test - Find Max P-Value");
		job.setJarByClass(HadoopIndependenceJob.class);
		job.setMapperClass(HadoopIndMaxPValueMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(HadoopIndMaxPValueReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);

		//Set input and output paths
		Path inputPath = new Path(conf.get("processedcounts"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("maxpvalues"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);	
	}

	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new HadoopIndependenceJob(), args);
		System.exit(exitCode);
	}
}
