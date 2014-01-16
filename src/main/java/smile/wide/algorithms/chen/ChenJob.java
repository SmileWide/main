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

import smile.wide.algorithms.independence.MinimumCutSet;
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
public class ChenJob extends Configured implements Tool {
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
    ArrayList<ArrayList<Set<Integer> > > sepsets = new ArrayList<ArrayList<Set<Integer> > >();
	Pattern pat = new Pattern();
	double epsilon = 0;
	Configuration conf = null;
	int nvar = 0;
	
	MinimumCutSet m = null;
	
	@Override
	public int run(String[] params) throws Exception {
		conf = super.getConf();
		//get number of variables
		nvar = conf.getInt("nvar", 0);
		//get pattern
		pat.setSize(nvar);
		//get epsilon
		epsilon = conf.getFloat("epsilon", 0);

		m = new MinimumCutSet(pat);
		
		// create sepsets
        for (int x=0; x< nvar; x++)
        	sepsets.add(new ArrayList<Set<Integer> >());
        for (int i = 0; i < nvar; i++)
            for (int x=0; x< nvar; x++)
            	sepsets.get(i).add(new HashSet<Integer>());
		
		/*from paper Chen et al. 2011:*/
 
		//Begin [Drafting]
		System.out.println("Begin Drafting");
		ArrayList<Pair<Pair<Integer,Integer>,Double>> L = new ArrayList<Pair<Pair<Integer,Integer>,Double>>();
		ArrayList<Pair<Pair<Integer,Integer>,Double>> removelist = new ArrayList<Pair<Pair<Integer,Integer>,Double>>();
		
		calculateMRMI(L);
		for(Pair<Pair<Integer,Integer>,Double> p : L) {
			int x = p.getFirst().getFirst();
			int y = p.getFirst().getSecond();
			if(!adjacencyPaths(x,y)) {
				pat.setEdge(x, y, EdgeType.Undirected);
				pat.setEdge(y, x, EdgeType.Undirected);
				removelist.add(p);
			}
		}
		for(Pair<Pair<Integer,Integer>,Double> p : removelist)
			L.remove(p);

		//Begin [Thickening]
		System.out.println("Begin Thickening");
		for(Pair<Pair<Integer,Integer>,Double> p : L) {
			int x = p.getFirst().getFirst();
			int y = p.getFirst().getSecond();
			System.out.println("-edgeNeeded("+x+","+y+")");
			if(edgeNeeded(x,y)) {
				pat.setEdge(x, y, EdgeType.Undirected);
				pat.setEdge(y, x, EdgeType.Undirected);
			}
		}

		//Begin [Thinning]
		System.out.println("Begin thinning");
		for(int x=0;x<pat.getSize();++x) {
			for(int y=x+1;y<pat.getSize();++y) {
				if(pat.getEdge(x,y)==EdgeType.Undirected) {
					if(adjacencyPaths(x,y)) {
						pat.setEdge(x, y, EdgeType.None);
						pat.setEdge(y, x, EdgeType.None);
						System.out.println("--edgeNeeded("+x+","+y+")");
						if(edgeNeeded(x,y)) {
							pat.setEdge(x, y, EdgeType.Undirected);
							pat.setEdge(y, x, EdgeType.Undirected);
						}
					}
				}
			}
		}
		System.out.println("Orient Edges");
		orientEdges();
		pat.Print();
		return 0;
	}

	boolean depthFirstSearch(Set<Integer> marked, int current, int target) {
		if( !marked.contains(current) ){
			marked.add(current);
			if(pat.getEdge(current, target) != Pattern.EdgeType.None)
				return true;
			for(int i = 0; i < pat.getSize();++i) {
				if(pat.getEdge(current, i) != Pattern.EdgeType.None) {
					if(depthFirstSearch(marked,i,target));
						return true;
				}
			}
		}
		return false;
	}

	boolean adjacencyPaths(int x, int y) {
		boolean edge_exists = false;
		if(pat.getEdge(x, y) != EdgeType.None) {
			edge_exists = true;
			pat.setEdge(x, y, EdgeType.None);
			pat.setEdge(y, x, EdgeType.None);
		}
		Set<Integer> marked = new HashSet<Integer>();
		boolean result = depthFirstSearch(marked, x, y);
		if(edge_exists) {
			pat.setEdge(x, y, EdgeType.Undirected);
			pat.setEdge(y, x, EdgeType.Undirected);
		}
		return result;
	}

/*	

	boolean depthFirstSearchPaths(Set<Integer> marked, Set<Integer> result, int current, int target) {
		boolean onpath = false;
		if( !marked.contains(current) ){
			marked.add(current);
			if(pat.getEdge(current, target) != Pattern.EdgeType.None)
			{
				result.add(current);
				return true;
			}
			for(int i = 0; i < pat.getSize();++i) {
				if(pat.getEdge(current, i) != Pattern.EdgeType.None) {
					if(depthFirstSearchPaths(marked,result,i,target)) {
						onpath = true;
						result.add(current);
					}
				}
			}
		}
		return onpath;
	}

	Set<Integer> minimumCutSet(int x, int y) {
		boolean edge_exists = false;
		if(pat.getEdge(x, y) != EdgeType.None) {
			edge_exists = true;
			pat.setEdge(x, y, EdgeType.None);
			pat.setEdge(y, x, EdgeType.None);
		}
		
		Set<Integer> Xnbr = new HashSet<Integer>();
		Set<Integer> Ynbr = new HashSet<Integer>();
		Set<Integer> SX = new HashSet<Integer>();
		Set<Integer> SY = new HashSet<Integer>();

		for(int i=0;i<pat.getSize();++i) {
			if(pat.getEdge(x, i) != EdgeType.None) {
				Xnbr.add(i);
				SX.add(i);
			}
			if(pat.getEdge(y, i) != EdgeType.None) {
				Ynbr.add(i);
				SY.add(i);
			}
		}
		
		Set<Integer> marked = new HashSet<Integer>();
		Set<Integer> adjpath = new HashSet<Integer>();
		depthFirstSearchPaths(marked,adjpath,x,y);
		adjpath.remove(x);

		SX.retainAll(adjpath);
		SY.retainAll(adjpath);

		//get neighbours of i from Sx, substract Sx from neighbours
		Set<Integer> temp = new HashSet<Integer>();
		Set<Integer> SXp = new HashSet<Integer>();
		for(Integer j : SX) {
			temp.clear();
			for(int i=0;i<pat.getSize();++i) {
				if(pat.getEdge(j, i) != EdgeType.None) {
					temp.add(i);
				}
			}			
			temp.removeAll(SX);
			SXp.addAll(temp);
		}
		SXp.retainAll(adjpath);

		//get neighbours of i from Sy, substract Sy from neighbours
		Set<Integer> SYp = new HashSet<Integer>();
		for(Integer j : SY) {
			temp.clear();
			for(int i=0;i<pat.getSize();++i) {
				if(pat.getEdge(j, i) != EdgeType.None) {
					temp.add(i);
				}
			}			
			temp.removeAll(SY);
			SYp.addAll(temp);
		}
		SYp.retainAll(adjpath);
		
		//return smallest of two subsets
		Set<Integer> result = null;
		if(SXp.size() < SYp.size())
			result = SXp;
		else
			result = SYp;
		
		if(edge_exists) {
			pat.setEdge(x, y, EdgeType.Undirected);
			pat.setEdge(y, x, EdgeType.Undirected);
		}
		return result;
	}
*/
	boolean edgeNeeded(int x, int y) throws Exception {
		Set<Integer> cutset = m.minimumCutSet(x,y);
		System.out.println("cutset: "+cutset);
		double IxyZ = 2*epsilon;
		if(!cutset.isEmpty())
			IxyZ = calculateMRCMI(x,y,cutset);
		if(IxyZ < epsilon) {
			sepsets.get(x).set(y,cutset);
			sepsets.get(y).set(x,cutset);
			return false;
		}
		return true;
	}
    private boolean sepsetHas(ArrayList<ArrayList<Set<Integer> > > sepsets, int x, int y, int e)
    {
        // check if the given sepset contains element e
        Set<Integer> sepset = sepsets.get(x).get(y);
        if(sepset == null)
        	return false;
        return sepset.contains(e);
    }
    
	void orientEdges() {
		//Their orientation rules are exactly the same as PC
        //orient edges as v-structure
        for (int i = 0; i < nvar; i++)
        {
            for (int adj1 = 0; adj1 < nvar; adj1++)
            {
                if (i != adj1 && (pat.getEdge(adj1, i) != Pattern.EdgeType.None || pat.getEdge(i, adj1) != Pattern.EdgeType.None))
                {
                    for (int adj2 = adj1 + 1; adj2 < nvar; adj2++)
                    {
                        if (i != adj2 && (pat.getEdge(adj2, i) != Pattern.EdgeType.None || pat.getEdge(i, adj2) != Pattern.EdgeType.None))
                        {
                            if (pat.getEdge(adj1, adj2) == Pattern.EdgeType.None && pat.getEdge(adj2, adj1) == Pattern.EdgeType.None && !sepsetHas(sepsets, adj1, adj2, i))
                            {
                                pat.setEdge(adj1, i, Pattern.EdgeType.Directed);
                                pat.setEdge(adj2, i, Pattern.EdgeType.Directed);
                                if (pat.getEdge(i, adj1) == Pattern.EdgeType.Undirected)
                                {
                                    pat.setEdge(i, adj1, Pattern.EdgeType.None);
                                }
                                if (pat.getEdge(i, adj2) == Pattern.EdgeType.Undirected)
                                {
                                    pat.setEdge(i, adj2, Pattern.EdgeType.None);
                                }
                            }
                        }
                    }
                }
            }
        }
        //orient remaining edges
        boolean update = true;
        while (update)
        {
            update = false;
            // a) orient x -> y - z as x -> y -> z
            for (int i = 0; i < nvar; i++)
            {
                for (int adj1 = 0; adj1 < nvar; adj1++)
                {
                    for (int adj2 = adj1 + 1; adj2 < nvar; adj2++)
                    {
                        if (i != adj1 && i != adj2 && pat.getEdge(adj1, i) == Pattern.EdgeType.Directed && pat.getEdge(adj2, i) == Pattern.EdgeType.Undirected && pat.getEdge(adj1, adj2) == Pattern.EdgeType.None && pat.getEdge(adj2, adj1) == Pattern.EdgeType.None)
                        {
                            if (!pat.hasDirectedPath(adj2, i))
                            {
                                pat.setEdge(i, adj2, Pattern.EdgeType.Directed);
                                pat.setEdge(adj2, i, Pattern.EdgeType.None);
                                update = true;
                            }
                        }
                        else
                        {
                            if (i != adj1 && i != adj2 && pat.getEdge(adj2, i) == Pattern.EdgeType.Directed && pat.getEdge(adj1, i) == Pattern.EdgeType.Undirected && pat.getEdge(adj1, adj2) == Pattern.EdgeType.None && pat.getEdge(adj2, adj1) == Pattern.EdgeType.None)
                            {
                                if (!pat.hasDirectedPath(adj1, i))
                                {
                                    pat.setEdge(i, adj1, Pattern.EdgeType.Directed);
                                    pat.setEdge(adj1, i, Pattern.EdgeType.None);
                                    update = true;
                                }
                            }
                        }
                    }
                }
            }
            // b) orient x - z as x -> z if there is a path x -> ... -> z
            for (int x = 0; x < nvar; x++)
            {
                for (int y = x + 1; y < nvar; y++)
                {
                    if (pat.getEdge(x, y) == Pattern.EdgeType.Undirected)
                    {
                        // search for directed path from x -> y and y -> x
                        boolean xy = pat.hasDirectedPath(x, y);
                        boolean yx = pat.hasDirectedPath(y, x);
                        if (xy && yx)
                        {
                            assert(false);
                        }
                        else
                        {
                            if (xy)
                            {
                                pat.setEdge(x, y, Pattern.EdgeType.Directed);
                                pat.setEdge(y, x, Pattern.EdgeType.None);
                                update = true;
                            }
                            else
                            {
                                if (yx)
                                {
                                    pat.setEdge(y, x, Pattern.EdgeType.Directed);
                                    pat.setEdge(x, y, Pattern.EdgeType.None);
                                    update = true;
                                }
                            }
                        }
                    }
                }
            }
        }
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
		job.setJarByClass(ChenJob.class);
		job.setMapperClass(ChenMICounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setCombinerClass(ChenCounterReducer.class);
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
						double Nxy = 0;
						double Nx = 0;
						double Ny = 0;
						if(counts.get(assignment_x+"+"+assignment_y) != null)
						{
							Nxy = counts.get(assignment_x+"+"+assignment_y);
							Nx = counts.get(assignment_x);
							Ny = counts.get(assignment_y);
						}
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
	
	double calculateMRCMI(int x, int y, Set<Integer> Z) throws Exception {
		//pass parameters
		conf.setInt("Vx", x);
		conf.setInt("Vy", y);
		String ssZ = "";
		boolean first = true;
		for(Integer i : Z) {
			if(first) {
				first=false;
				ssZ = i.toString();
			}
			else
				ssZ += ","+i;
		}
		conf.set("parents", ssZ);
		
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Conditional Mutual Information - Calculate Counts: N("+x+","+y+"|"+Z+")");
		job.setJarByClass(ChenJob.class);
		job.setMapperClass(ChenCMICounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setCombinerClass(ChenCounterReducer.class);
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
		ArrayList<String> varids = new ArrayList<String>();
		ArrayList<Integer> varZ = new ArrayList<Integer>();
		for(Integer z : Z) {
			varids.add("v"+z);
			varZ.add(z);
		}
		ArrayList<Iterator<String>> indices = new ArrayList<Iterator<String>>();

		double IxyZ = 0;
		Iterator<String> itX = cardinalities.get(x).iterator();
		while(itX.hasNext()) {
			String assignment_x = "v"+x+"="+itX.next();
			Iterator<String> itY = cardinalities.get(y).iterator();
			while(itY.hasNext()) {
				String assignment_y = "v"+y+"="+itY.next();

				indices.clear();
				for(Integer z : Z)
					indices.add(cardinalities.get(z).iterator());
				indices.add(null);
				ArrayList<String> cur_values = new ArrayList<String>();
				for(int i=0;i<indices.size()-1;++i) {
					cur_values.add(indices.get(i).next());
				}
				boolean done =false;
				while(!done) {
					int cntr=0;
					String assignment_Z="";
					for(String z: cur_values) {
						if(cntr==0)
							assignment_Z=varids.get(cntr)+"=";
						else
							assignment_Z+="+"+varids.get(cntr)+"=";
						assignment_Z+= z;
						++cntr;
					}
					double NxyZ = 0;
					double NxZ = 0;
					double NyZ = 0;
					double NZ = 0;
					if(counts.get(assignment_x+"+"+assignment_y+"+"+assignment_Z) != null) {
						NxyZ = counts.get(assignment_x+"+"+assignment_y+"+"+assignment_Z);
						NxZ = counts.get(assignment_x+"+"+assignment_Z);
						NyZ = counts.get(assignment_y+"+"+assignment_Z);
						NZ = counts.get(assignment_Z);
					}
					if(NxyZ > 0)
						IxyZ += (NxyZ / NZ)*(Math.log(NxyZ)+Math.log(NZ)-Math.log(NxZ)-Math.log(NyZ));
					for(int j=0;j<indices.size();++j) {
						if(indices.get(j)==null) {
							done = true;
							break;
						}
						if(indices.get(j).hasNext()) {
							cur_values.set(j, indices.get(j).next());
							break;
						}
						else {
							indices.set(j, cardinalities.get(varZ.get(j)).iterator());
							cur_values.set(j, indices.get(j).next());
						}
					}
				}
			}
		}
		return IxyZ;
	}
		
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("nvar", 70);
		conf.setFloat("epsilon", (float) 0.01);
		conf.set("datainput", "/user/mdejongh/input");
		conf.set("countoutput", "/user/mdejongh/counts");
		conf.set("countlist","mycounts.txt");
		
		int exitCode = ToolRunner.run(conf, new ChenJob(), args);
		System.exit(exitCode);
	}
}
