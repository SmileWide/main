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
import java.util.regex.Matcher;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import smile.wide.data.SMILEData;
import smile.wide.hadoop.io.ItestPairArrayWritable;
import smile.wide.hadoop.io.PartitionInputFormat;
import smile.wide.utils.Pattern;
import smile.wide.utils.Pattern.EdgeType;

/** 
 * @author m.a.dejongh@gmail.com
 */
public class PartitionIndependenceJob extends Configured implements Tool {
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/

	/** Path to SMILE library*/
	private String libHDFSPath_ = "/user/mdejongh/lib/linux64";
	int maxmaps = 2000;
	int maxreds = 100;
	public SMILEData data = null;
	public Pattern pat = null;
	public ArrayList<ArrayList<Set<Integer>>> sepsets = null;
	private String tmpdata = "";
	private boolean data_uploaded = false;
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

		if(!data_uploaded) {
			if(dfs.exists(datapath)) {
				dfs.delete(datapath, true);
			}
			dfs.mkdirs(datapath);
			dfs.copyFromLocalFile(new Path("tmpdata.txt"),datapath);
			data_uploaded = true;
		}
		DistributedCache.addCacheFile(new URI(datapath + "/tmpdata.txt#tmpdata.txt"), conf);		
		DistributedCache.createSymlink(conf);
		
		//maybe add procedure to upload smile (for now library is parked on cluster)
		DistributedCache.addFileToClassPath(new Path(libHDFSPath_ + "/smile.jar"), conf);
		DistributedCache.addCacheFile(
				new URI(libHDFSPath_ + "/libjsmile.so#libjsmile.so"), conf);
		//rather not have this, but need the mahout jar
		DistributedCache.addFileToClassPath(new Path("/user/mdejongh/lib/mahout-core-0.8.jar"), conf);
		
		conf.set("thedata","tmpdata.txt");
		conf.set("edgelist","edgelist.txt");
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec"); 
		conf.setLong("mapred.task.timeout", 14400000);
		conf.set("mapred.child.java.opts","-Xmx4096m");
		int maxAdjacency = conf.getInt("maxAdjacency",0);

		for(int adjacency = 0; adjacency <= maxAdjacency;++adjacency) {
			double edges = 0.0;
			for(int x=0;x<pat.getSize();++x) {
				for(int y=x+1;y<pat.getSize();++y) {
					if(pat.getEdge(x,y)==EdgeType.Undirected)
						edges+=1;
				}
			}
			double maps = (double) maxmaps;
			int K = (int) Math.ceil(edges/maps);
			maps = Math.ceil(edges/K);
			maxmaps = (int) maps;
			maxreds = ((int) (0.1 * maps)) + 1; 
			System.out.println("edges "+edges);
			System.out.println("maps "+maps);
			System.out.println("value of K: "+K);
			conf.setInt("numberoftests",K);
			//init RandSeedInputFormat
			conf.setInt(PartitionInputFormat.CONFKEY_MAP_COUNT, maxmaps);//number of mappers to be run
			conf.setInt(PartitionInputFormat.CONFKEY_INTERVAL_COUNT, K);//number of mappers to be run
			conf.setInt("maxreds",maxreds);
			testIndependence(conf,adjacency);
		}
		f.delete();
		return 0;
	}

	void testIndependence(Configuration conf, int adjacency) throws Exception {
		conf.setInt("adjacency",adjacency);

		//upload current pattern state
		System.out.println("Uploading pattern");
		uploadPattern(conf);
		
		//init job
		System.out.println("init job");
		Job job = new Job(conf);
		job.setJobName("Partition Independence Tests Over Mappers and Collect Results, phase " + adjacency);
		job.setJarByClass(PartitionIndependenceJob.class);
		job.setMapperClass(PartitionTestMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ItestPairArrayWritable.class);
		job.setCombinerClass(IndependenceTestReducer.class);
		job.setReducerClass(IndependenceTestReducer.class);
		job.setInputFormatClass(PartitionInputFormat.class);
		job.setNumReduceTasks(maxreds);

		System.out.println("set output paths");
		//Set output paths
		Path outputPath = new Path(conf.get("testoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		//Run the job
		System.out.println("Starting Job");
		job.waitForCompletion(true);	
		//download result file
		FileSystem dfs = FileSystem.get(conf);
		//if we have more than one reducer, loop over all.
		String outputfile = conf.get("edgelist");
		FileUtil.copyMerge(dfs, outputPath, dfs, outputPath.suffix("/"+outputfile), false, conf, null);
		dfs.copyToLocalFile(outputPath.suffix("/"+outputfile), new Path("./"+outputfile));
		//process downloaded file
		java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\(\\((\\d+), (\\d+)\\),\\{((,?\\d+)*)\\}\\)");
		try {
			File file = new File(outputfile);
			String data = FileUtils.readFileToString(file);
			int counter = 0;
			Matcher matcher = p.matcher(data);
			while(matcher.find()) {
				int from = Integer.parseInt(matcher.group(1));
				int to = Integer.parseInt(matcher.group(2));
				String sep = matcher.group(3);
				pat.setEdge(from, to, EdgeType.None);
				pat.setEdge(to, from, EdgeType.None);
				if(adjacency > 0 && sep != "") {
					String[] separators = sep.split(",");
					Set<Integer> sepset = new HashSet<Integer>();
					for(int x=0;x<separators.length;++x)
						sepset.add(Integer.decode(separators[x]));
					sepsets.get(from).set(to,sepset);
					sepsets.get(to).set(from,sepset);
				}
				counter++;
			}
			System.out.println("removed "+counter+" edges");
		file.delete();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	void uploadPattern(Configuration conf) throws Exception {
		String patfile = "tmppat.txt";
		String patstring = pat.toString();
		PrintWriter out = new PrintWriter(patfile);
		out.println(patstring);
		patstring=null;
		out.close();
		FileSystem dfs = FileSystem.get(conf);
		Path datapath = new Path(tmpdata);
		dfs.copyFromLocalFile(new Path(patfile),datapath);
		DistributedCache.addCacheFile(new URI(datapath + "/"+patfile+"#"+patfile), conf);		
		DistributedCache.createSymlink(conf);
	}
}
