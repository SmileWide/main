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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	double epsilon = 0;
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
		epsilon = conf.getFloat("epsilon", 0);

		//Fang's algorithm
		
		pat.Print();
		return 0;
	}
	
	class MIComparator implements Comparator<Pair<Pair<Integer,Integer>,Double>> {
		/**compare function*/
		@Override
		public int compare(Pair<Pair<Integer,Integer>,Double> arg0, Pair<Pair<Integer,Integer>,Double> arg1) {
			return arg0.getSecond().compareTo(arg1.getSecond());
		}
	}
	
	void calculateMRMI(ArrayList<Pair<Pair<Integer,Integer>,Double>> L) throws Exception {
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Mutual Information - Calculate Counts");
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

		//get total nr of records
		double N = 0;
		for(String s : cardinalities.get(0)) {
			N+=counts.get("v0="+s);
		}
		/*
		Calculate_MI:
		//I(X,Y) = Sum_{x,y}P(x,y)log({P(x,y)}/{P(x)P(y)})
		 */
		for(int x = 0; x<pat.getSize();++x) {
			for(int y=x+1;y<pat.getSize();++y) {
				double Ixy = 0;
				Iterator<String> itX = cardinalities.get(x).iterator();
				while(itX.hasNext()) {
					String assignment_x = "v"+x+"="+itX.next();
					Iterator<String> itY = cardinalities.get(y).iterator();
					while(itY.hasNext()) {
						String assignment_y = "v"+y+"="+itY.next();
						double Nxy = counts.get(assignment_x+"+"+assignment_y);
						double Nx = counts.get(assignment_x);
						double Ny = counts.get(assignment_y);
						if(Nxy > 0)
							Ixy += (Nxy / N)*(Math.log(Nxy)+Math.log(N)-Math.log(Nx)-Math.log(Ny));
					}
				}
				if(Ixy > epsilon)
					L.add(new Pair<Pair<Integer,Integer>,Double>(new Pair<Integer,Integer>(x,y),Ixy));
			}
		}
		//Sort L into decreasing order
		Collections.sort(L, new MIComparator());
	}
			
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("nvar", 179);
		conf.setInt("maxsetsize", 8);
		conf.set("datainput", "somewhere");
		conf.set("countoutput", "somewhere");
		conf.set("countlist","mycounts.txt");
		
		int exitCode = ToolRunner.run(conf, new FangJob(), args);
		System.exit(exitCode);
	}
}
