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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.hadoop.io.RandSeedInputFormat;
import smile.wide.hadoop.io.StrucLogLikeWritable;


/** Job class to run SMILE Bayesian Search on a hadoop cluster
 *  Sets up Hadoop configuration
 *  Distributes SMILE .jar and .so file
 *  Distributes temp data file
 *  Starts MapReduce job
 *  Retrieves output data file
 * @author m.a.dejongh@gmail.com
 */
public class SMILEBSjob extends Configured implements Tool {
	//parameter for Bayesian search
	/** Number of iterations the algorithm is run, after an iteration is finished a random restart occurs*/
	private int iterationCount = 20;
	/** Probability of generating an arc between two nodes when performing the random restart*/
	private float linkProbability = 0.01f;
	/** Limits the number of parents a node can have*/
	private int maxParents = 8;
	/** Limits the amount of time the algorithm can spend on searching for a network structure (0 is unlimited)*/
	private int maxSearchTime = 0;
	/** Influences prior probability of network structures during Bayesian Search*/
	private float priorLinkProbability = 0.001f;
	/** Equivalent sample size for Bayesian Search, larger values increase parameter resistance to data*/
	private int priorSampleSize = 50;
	
	//paths for SMILE location and Input Data (txt file to be read into SMILE)
	//Stole this from Tomas for now. We need something better
	//placeholder for better idea
	//We need a more standardized approach
	/** Path to SMILE library*/
	private String libHDFSPath_ = "/user/mdejongh/lib/linux64";
	/** Path to data storage */
	private String dataHDFSPath_ = "/user/mdejongh/data/"; 

	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
	@Override
	public int run(String[] params) throws Exception {
		//params: <trainfile> <output_path> <number of seeds>
		Configuration conf = super.getConf();
		conf.set("trainfile", params[0]);
		//distributed cache initialization
		DistributedCache.createSymlink(conf);

		DistributedCache.addFileToClassPath(new Path(libHDFSPath_ + "/smile.jar"), conf);
		DistributedCache.addCacheFile(
				new URI(libHDFSPath_ + "/libjsmile.so#libjsmile.so"), conf);
		//upload data file to HDFS and add it to the distributed cache
		FileSystem dfs = FileSystem.get(conf);
		dfs.copyFromLocalFile(new Path(params[0]),new Path(dataHDFSPath_));
		DistributedCache.addCacheFile(
				new URI(dataHDFSPath_ + basename(params[0]) + "#"  + basename(params[0])), conf);
		
		//for now, keep the Bayesian search parameters constant
		conf.setInt("iterationCount",iterationCount);
		conf.setFloat("linkProbability", linkProbability);
		conf.setInt("maxParents",maxParents);
		conf.setInt("maxSearchTime",maxSearchTime);
		conf.setFloat("priorLinkProbability", priorLinkProbability);
		conf.setInt("priorSampleSize", priorSampleSize);
		//
		conf.setInt(RandSeedInputFormat.CONFKEY_SEED_COUNT, Integer.parseInt(params[2]));
		conf.setInt(RandSeedInputFormat.CONFKEY_WARMUP_ITER, 100000);
		conf.setLong("mapred.task.timeout", 3600000);
		
		Job job = new Job(conf);
		job.setJobName("Distributed Bayesian Search");
		job.setJarByClass(SMILEBSjob.class);
		job.setMapperClass(SMILEBSMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StrucLogLikeWritable.class);
		job.setReducerClass(SMILEBSReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(RandSeedInputFormat.class);
		Path outputPath = new Path(params[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);
		//now download result
		outputPath.suffix("/part-r-00000");
		dfs.copyToLocalFile(outputPath.suffix("/part-r-00000"), new Path("./smile-output.txt"));
		return 0;
	}
	
	/** Returns basename of file*/
	private String basename(String fName) {		
		return fName.substring(fName.lastIndexOf('/')+1); 
	}

	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new SMILEBSjob(), args);
		System.exit(exitCode);
	}

	public int getIterationCount() {
		return iterationCount;
	}

	public void setIterationCount(int iterationCount) {
		this.iterationCount = iterationCount;
	}

	public float getLinkProbability() {
		return linkProbability;
	}

	public void setLinkProbability(float linkProbability) {
		this.linkProbability = linkProbability;
	}

	public int getMaxParents() {
		return maxParents;
	}

	public void setMaxParents(int maxParents) {
		this.maxParents = maxParents;
	}

	public int getMaxSearchTime() {
		return maxSearchTime;
	}

	public void setMaxSearchTime(int maxSearchTime) {
		this.maxSearchTime = maxSearchTime;
	}

	public float getPriorLinkProbability() {
		return priorLinkProbability;
	}

	public void setPriorLinkProbability(float priorLinkProbability) {
		this.priorLinkProbability = priorLinkProbability;
	}

	public int getPriorSampleSize() {
		return priorSampleSize;
	}

	public void setPriorSampleSize(int priorSampleSize) {
		this.priorSampleSize = priorSampleSize;
	}
}
