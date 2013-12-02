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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
		conf = super.getConf();

		//get number of variables
		nvar = conf.getInt("nvar", 0);
		//get pattern
		pat.setSize(nvar);
		//get epsilon
		maxsetsize = conf.getInt("maxsetsize", 0);

		//Fang's algorithm (possibly, it's not completely clear from their paper)
		for(int i=0;i<nvar;++i) {
			Set<Integer> parents = new HashSet<Integer>();
			double Pold = calculateScore(i);//Score for empty parent set
			boolean OkToProceed = true;
			while(OkToProceed && parents.size() < maxsetsize) {
				Pair<Integer,Double> max = new Pair<Integer,Double>();
				findBestCandidate(i,parents,max);//For each candidate calculate score, find max in reducers
				if(max.getSecond() > Pold) {
					Pold = max.getSecond();
					parents.add(max.getFirst());
					pat.setEdge((max.getFirst()), i, EdgeType.Directed);
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
		
		//we need to pass v
		conf.setInt("VarX", v);
		
		//init job
		Job job = new Job(conf);
		job.setJobName("K2 - Calculate Counts");
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

		/*
		 * We need to calculate K2 here.
		 * This is based on the no parents case.
		 * Simplest case.
		 *
		 * general version
		 * g(i,PIi) = PROD^{n}_{i=1}PROD^{Qi}_{j=1}{(Ri-1)!}/{(Nij+Ri-1)!}PROD^{Ri}_{k=1}Nijk!
		 * 
		 * Empty parent version (one node)
		 * g(i,[]) = {(R-1)!}/{(N+R-1)!} * PROD^{R}_{k=1}Nk!
		 */

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
		//calculate score
		double logSum = logNk + ArithmeticUtils.factorialLog((int)(R-1));;
		logSum -= ArithmeticUtils.factorialLog((int)(N+R-1));
		return logSum;
	}
	
	
	void findBestCandidate(int x, Set<Integer> parents,Pair<Integer,Double> result) throws Exception {
/*
		Let Z be a node in Pred(Xi) - PIi maximizing g(i, PIi U {Zi}) 
		//effective try all of the available parent nodes (1 step at a time)
		MR2: Calculate counts for candidates
*/
		//init job
		Job job = new Job(conf);
		job.setJobName("K2 - Calculate Counts");
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
		//Calculate conditional counts
		
/*		
		MR3: pick best structure
 		MAP: calculate score for candidate structures
 		RED: pick max candidate structure (i.e. z that maximizes score) 
*/
	}
	
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("nvar", 70);
		conf.setInt("maxsetsize", 1);
		conf.set("datainput", "somewhere");
		conf.set("countoutput", "somewhere");
		conf.set("countlist","mycounts.txt");
		int exitCode = ToolRunner.run(conf, new FangJob(), args);
		System.exit(exitCode);
	}
}
