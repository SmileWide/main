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
package smile.wide.algorithms.chen;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class ChenIndependenceJob extends Configured implements Tool {
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
	Pattern pat = new Pattern();
	double epsilon = 0;
	@Override
	public int run(String[] params) throws Exception {
		Configuration conf = super.getConf();

		//get pattern
		pat.setSize(10);
		//get epsilon
		epsilon = 0.05;

		int nvar = pat.getSize();
        // create sepsets
        ArrayList<ArrayList<Set<Integer> > > sepsets = new ArrayList<ArrayList<Set<Integer> > >();
        for (int x=0; x< nvar; x++) {
        	sepsets.add(new ArrayList<Set<Integer> >());
        }
        for (int i = 0; i < nvar; i++)
        {
            for (int x=0; x< nvar; x++) {
            	sepsets.get(i).add(new HashSet<Integer>());
            }
        }
		
		
		/*from paper Chen et al. 2011:*/
 
		//Begin [Drafting]
		ArrayList<Pair<Pair<Integer,Integer>,Double>> L = new ArrayList<Pair<Pair<Integer,Integer>,Double>>();
		calculateMRMI(conf,L);
		for(Pair<Pair<Integer,Integer>,Double> p : L) {
			int x = p.getFirst().getFirst();
			int y = p.getFirst().getSecond();
			if(!adjacencyPaths(x,y)) {
				pat.setEdge(x, y, EdgeType.Undirected);
				pat.setEdge(y, x, EdgeType.Undirected);
				L.remove(p);
			}
		}
		//Begin [Thickening]
		for(Pair<Pair<Integer,Integer>,Double> p : L) {
			int x = p.getFirst().getFirst();
			int y = p.getFirst().getSecond();
			if(edgeNeeded(x,y)) {
				pat.setEdge(x, y, EdgeType.Undirected);
				pat.setEdge(y, x, EdgeType.Undirected);
			}
		}
		//Begin [Thinning]
		for(int x=0;x<pat.getSize();++x) {
			for(int y=x+1;y<pat.getSize();++y) {
				if(pat.getEdge(x,y)==EdgeType.Undirected) {
					if(adjacencyPaths(x,y)) {
						pat.setEdge(x, y, EdgeType.None);
						pat.setEdge(y, x, EdgeType.None);
						if(edgeNeeded(x,y)) {
							pat.setEdge(x, y, EdgeType.Undirected);
							pat.setEdge(y, x, EdgeType.Undirected);
						}
					}
				}
			}
		}
		orientEdges();
		return 0;
	}

	boolean adjacencyPaths(int x, int y) {
		/*
			AdjacencyPath
			Require: X, Y, and network
			Any path between X and Y ?
		 */
		return true;
	}
	
	boolean edgeNeeded(int x, int y) {
		/*
		EdgeNeeded
		Require <X,Y>, V={all attributes}, E={current edge list}
		D(<X,Y>)=MinimumCutSet(<X,Y>,E)
		NewThread(MR_CMI(<X,Y>,D(<X,Y>)))
		I(X,Y|D(<X,Y>)) = Calculate_CMI(<X,Y>,D(<X,Y>)))
		If I(X,Y|D(<X,Y>)) < epsilon
			return false
		else
			return true
		*/
		return true;
	}
		
	Set<Integer> minimumCutSet(int x, int y) {
		/*
			we need special case of mincutset that separates var X from var y (i.e. cutting all paths)
			l. if exists, temporarily remove edge between X and Y
			2. Find all neighbours from X and from Y
			3. Smallest set is mincutset??? 
			 
			--Stoer-Wagner--
			MINIMUMCUTPHASE(G, w, a)
			A <- {a}
			while A != V
				add to A the most tightly connected vertex
				store the cut-of-the-phase and shrink G by merging the two vertices added last
			
			Most tightly connected vertex:
			z !element A such that w(A,z) = max{w(A,y)|y !element A},
			
			MINIMUMCUT(G, w, a)
			while |V| > 1
				MINIMUMCUTPHASE(G, w, a)
				if the cut-of-the-phase is lighter than the current minimum cut
					then store the cut-of-the-phase as the current minimum cut

		 */
		HashSet<Integer> set = new HashSet<Integer>();
		return set;
	}
	
	void orientEdges() {
		/*
		OrientEdges (PC orientation rules???)
		Require: V,E
		For any three nodes X, Y and Z that X and Y , and Y and Z, are directly connected; and X and Z are not directly connected
			if {X, Z}, C ∈ CutSet and Y ∈ C, or {X, Z}, C ∈ CutSet
				let X be a parent of Y and let Z be a parent of Y .
		For any three nodes X, Y, Z, in V
			if (i) X is a parent of Y , (ii) Y and Z are adjacent,(iii) X and Z are not adjacent, and (iv) edge (Y, Z) is not oriented,
				let Y be a parent of Z.
		For any edge (X, Y ) that is not oriented.
			If there is a directed path from X to Y
				let X be a parent of Y .
		*/
	}
	
	void calculateMRMI(Configuration conf,ArrayList<Pair<Pair<Integer,Integer>,Double>> L) throws Exception {
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Mutual Information - Calculate Counts");
		job.setJarByClass(ChenIndependenceJob.class);
		job.setMapperClass(ChenMICounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setReducerClass(ChenCounterReducer.class);
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
		
		/*
		Calculate_MI:
		 */
		for(int x = 0; x<pat.getSize();++x) {
			for(int y=x+1;y<pat.getSize();++y) {
				//I(X,Y) = Sum_{x,y}P(x,y)log({P(x,y)}/{P(x)P(y)})
				double Ixy = 0;
				if(Ixy > epsilon)
					L.add(new Pair<Pair<Integer,Integer>,Double>(new Pair<Integer,Integer>(x,y),Ixy));
			}
		}
		//Sort L into decreasing order
	}
	
	double calculateMRCMI(Configuration conf, int x, int y, Set<Integer> Z) throws Exception {
		//pass parameters
		conf.setInt("Vx", x);
		conf.setInt("Vy", y);
		String[] sZ = Z.toArray(new String[0]);
		conf.setStrings("Z", sZ);
		
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Conditional Mutual Information - Calculate Counts");
		job.setJarByClass(ChenIndependenceJob.class);
		job.setMapperClass(ChenCMICounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setReducerClass(ChenCounterReducer.class);
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
		
		/*
		Calculate_CMI:
		I(A,B|C) = Sum_{a,b,c} P(a,b|c)log({P(a,b|C}/{P(a|c)P(b|c)})
		 */
		return 0.0;
	}
		
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new ChenIndependenceJob(), args);
		System.exit(exitCode);
	}
}
