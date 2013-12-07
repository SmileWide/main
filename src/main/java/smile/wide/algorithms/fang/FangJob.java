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
package smile.wide.algorithms.fang;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.utils.Pair;
import smile.wide.utils.PairDoubleWritable;
import smile.wide.utils.Pattern;
import smile.wide.utils.Pattern.EdgeType;

/** Job class to run PC's Independence tests on a hadoop cluster
 *  Sets up Hadoop configuration
 *  Distributes temp data file
 *  Starts MapReduce job
 *  Retrieves output data file
 * @author m.a.dejongh@gmail.com
 */
public class FangJob extends Configured implements Tool {
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
    ArrayList<ArrayList<Set<Integer> > > sepsets = new ArrayList<ArrayList<Set<Integer> > >();
	Pattern pat = new Pattern();
	int maxsetsize = 0;
	Configuration conf = null;
	int nvar = 0;
	
	@Override
	public int run(String[] params) throws Exception {
		//TODO add in variable ordering
		//get configuration
		conf = super.getConf();
		//get number of variables
		nvar = conf.getInt("nvar", 0);
		//get pattern
		pat.setSize(nvar);
		//get maxsetsize
		maxsetsize = conf.getInt("maxsetsize", 0);

		//Fang's algorithm (possibly, it's not completely clear from their paper)
		for(int i=0;i<nvar;++i) {
			System.out.println("Checking node "+i);
			Set<Integer> parents = new HashSet<Integer>();
			double Pold = calculateScore(i);//Score for empty parent set
			System.out.println(" Parentless score: "+Pold);
			boolean OkToProceed = true;
			while(OkToProceed && parents.size() < maxsetsize) {
				Pair<Integer,Double> max = new Pair<Integer,Double>();
				findBestCandidate(i,parents,max);//For each candidate calculate score, find max in reducers
				if(max.getSecond() > Pold) {
					Pold = max.getSecond();
					parents.add(max.getFirst());
					pat.setEdge((max.getFirst()), i, EdgeType.Directed);
					
					System.out.println(max);
				}
				else
					OkToProceed = false;
			}	
		}
		pat.Print();
		return 0;
	}
	
	double calculateScore(int v) throws Exception {
		//This is a very simple MR job
		//just get the counts for the variable.
		//We could potentially calculate this for all variables at once (but that's not how it's described in paper)
		
		System.out.println("Calculating score for "+v);
		
		//we need to pass v
		conf.setInt("VarX", v);
		
		//init job
		Job job = new Job(conf);
		job.setJobName("K2 - Calculate Counts for node " + v);
		job.setJarByClass(FangJob.class);
		job.setMapperClass(FangParentLessCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setCombinerClass(FangCounterReducer.class);
		job.setReducerClass(FangCounterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);

		//Set input and output paths
		Path inputPath = new Path(conf.get("datainput"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("countoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);
		
		//download result file
		FileSystem dfs = FileSystem.get(conf);
		outputPath.suffix("/part-r-00000");
		String outputfile = conf.get("countlist");
		dfs.copyToLocalFile(outputPath.suffix("/part-r-00000"), new Path("./"+outputfile));

		//Data structures for the counts in the file and node cardinalities
		Map<String,Integer> counts = new HashMap<String,Integer>();
		List<HashSet<String>> cardinalities = new ArrayList<HashSet<String>>();
		for(int i=0;i<nvar;++i)
			cardinalities.add(new HashSet<String>());
		
		//retrieve results here
		try {
			File file = new File(outputfile);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] contents = line.split("\t");
				counts.put(contents[0], Integer.decode(contents[1]));
				String[] assignments = contents[0].split("\\+");
				for(int i=0;i<assignments.length;++i) {
					String[] parts = assignments[i].split("=");
					int index = Integer.decode(parts[0].substring(1));
					cardinalities.get(index).add(parts[1]);
				}
			}
			fileReader.close();
			file.delete();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//get total nr of records
		double N = 0;
		double logNk = 0;
		for(String s : cardinalities.get(v)) {
			double count = counts.get("v"+v+"="+s);
			N+=count;
			logNk +=  ArithmeticUtils.factorialLog((int)count);
		}

		//get node cardinality
		double R = cardinalities.get(v).size();

		//calculate K2 score
		double logSum = logNk + ArithmeticUtils.factorialLog((int)(R-1));
		logSum -= ArithmeticUtils.factorialLog((int)(N+R-1));

		//return result
		return logSum;
	}
	
	
	void findBestCandidate(int x, Set<Integer> parents,Pair<Integer,Double> result) throws Exception {
		//MR2: Calculate counts for candidates

		//set node
		conf.setInt("VarX", x);
		
		//set parents
		String par = "";
		boolean first = true;
		for(Integer i : parents) {
			if(first) {
				first=false;
				par = i.toString();
			}
			else
				par += ","+i;
		}
		conf.set("parents", par);
		
		//init job
		Job job = new Job(conf);
		job.setJobName("K2 - Calculate Counts node "+x+" parents "+parents+" and candidates");
		job.setJarByClass(FangJob.class);
		job.setMapperClass(FangCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setCombinerClass(FangCounterReducer.class);
		job.setReducerClass(FangCounterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);

		//Set input and output paths
		Path inputPath = new Path(conf.get("datainput"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("countoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);
		
		//check if file was created previously and delete it if so
		FileSystem dfs = FileSystem.get(conf); 
		Path source = new Path(outputPath+"/part-r-00000");
		Path sink = new Path(outputPath+"/../thecountfile");
		if (dfs.exists(sink))
			dfs.delete(sink);
		outputPath.getFileSystem(conf).rename(source, sink);
		
		//add the result file to the Distributed cache and create symbolic links
		DistributedCache.addCacheFile(new URI(outputPath + "/../thecountfile#thecountfile"), conf);		
		DistributedCache.createSymlink(conf);

		//MR3: pick best structure
		//node and parents are already set an can be reused
		conf.set("countfile", "thecountfile");
		
		//init job
		job = new Job(conf);
		job.setJobName("K2 - Score Candidate Parent Additions for Node " + x);
		job.setJarByClass(FangJob.class);
		job.setMapperClass(FangStructureScoreMapper.class);
		job.setMapOutputKeyClass(VIntWritable.class);
		job.setMapOutputValueClass(PairDoubleWritable.class);
		job.setCombinerClass(FangStructureScoreReducer.class);
		job.setReducerClass(FangStructureScoreReducer.class);
		job.setInputFormatClass(StructureInputFormat.class);
		job.setNumReduceTasks(1);

		//output path should be best change (most compact, number and score (increase?))
		outputPath = new Path(conf.get("structureoutput"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);	
		
		//download result file
		outputPath.suffix("/part-r-00000");
		String structurefile = conf.get("beststructure");
		dfs.copyToLocalFile(outputPath.suffix("/part-r-00000"), new Path("./"+structurefile));
		
		//here read file, collect best modification and update network.
		int bestcandidate = -1;
		double bestscore = 1.0;
		try {
			File file = new File(structurefile);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			if ((line = bufferedReader.readLine()) != null) {
				String[] contents = line.split("\t");
				bestcandidate = (int) Double.parseDouble(contents[1]);
				bestscore = Double.parseDouble(contents[2]);
			}
			fileReader.close();
			file.delete();
		} catch (IOException e) {
			e.printStackTrace();
		}
		result.setFirst(bestcandidate);
		result.setSecond(bestscore);
	}
	
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("nvar", 70);
		conf.setInt("maxsetsize", 1);
		conf.set("datainput", "/user/mdejongh/input");
		conf.set("countoutput", "/user/mdejongh/counts");
		conf.set("structureoutput","/user/mdejongh/beststructure");
		conf.set("countlist","mycounts.txt");
		conf.set("beststructure","beststructure.txt");
		int exitCode = ToolRunner.run(conf, new FangJob(), args);
		System.exit(exitCode);
	}
}
