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

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import smile.wide.data.SMILEData;
import smile.wide.hadoop.io.RandSeedInputFormat;
import smile.wide.utils.Pattern;

/** 
 * @author m.a.dejongh@gmail.com
 */
public class DistributedIndependenceJob extends Configured implements Tool {
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
	
	public SMILEData data = null;
	public Pattern pat = null;
	public ArrayList<ArrayList<Set<Integer>>> sepsets = null;
	private String tmpdata = "";
	@Override
	public int run(String[] params) throws Exception {
		Configuration conf = super.getConf();
		File f = new File("tmpdata.txt");
		if(!f.exists()) {
			data.Write("tmpdata.txt");
		}
		tmpdata = conf.get("datastorage","");
		FileSystem dfs = FileSystem.get(conf);
		Path datapath = new Path(tmpdata);

		if(dfs.exists(datapath)) {
			dfs.delete(datapath, true);
		}
		dfs.mkdirs(datapath);
		dfs.copyFromLocalFile(new Path("tmpdata.txt"),datapath);
		DistributedCache.addCacheFile(new URI(datapath + "/tmpdata.txt#tmpdata.txt"), conf);		
		DistributedCache.createSymlink(conf);
		
		//maybe add procedure to upload smile (for now will use command line)
		
		conf.set("thedata","tmpdata.txt");
		conf.set("edgelist","edgelist.txt");
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec"); 
		conf.setInt(RandSeedInputFormat.CONFKEY_SEED_COUNT, Integer.parseInt("500"));
		conf.setInt(RandSeedInputFormat.CONFKEY_WARMUP_ITER, 100000);

		int maxAdjacency = conf.getInt("maxAdjacency",0);

		for(int adjacency = 0; adjacency <= maxAdjacency;++adjacency) {
			testIndependence(conf,adjacency);
		}
		f.delete();
		return 0;
	}

	void testIndependence(Configuration conf, int adjacency) throws Exception {
		//init job
		Job job = new Job(conf);
		job.setJobName("Distribute Independence Tests Over Mappers and Collect Results");
		job.setJarByClass(DistributedIndependenceJob.class);
		job.setMapperClass(IndependenceTestMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setCombinerClass(HadoopIndCounterReducer.class);
		job.setReducerClass(HadoopIndCounterReducer.class);
		job.setInputFormatClass(RandSeedInputFormat.class);
		job.setNumReduceTasks(1);

		//Set output paths
		Path outputPath = new Path(conf.get("testoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		//Run the job
		job.waitForCompletion(true);	
		//download result file
		FileSystem dfs = FileSystem.get(conf);
		outputPath.suffix("/part-r-00000");
		String outputfile = conf.get("edgelist");
		dfs.copyToLocalFile(outputPath.suffix("/part-r-00000"), new Path("./"+outputfile));
		//process downloaded file
		//update the pattern
	}
}
