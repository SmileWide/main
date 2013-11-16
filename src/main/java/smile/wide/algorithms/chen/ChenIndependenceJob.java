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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
public class ChenIndependenceJob extends Configured implements Tool {
	/** Sets up the hadoop job and sends it to the cluster
	 * waits for the job to be completed.*/
	@Override
	public int run(String[] params) throws Exception {
		Configuration conf = super.getConf();

		/*for simplicity implement complete algorithm here.
		/*from paper Chen et al. 2011:
		 * 
		    MainControllerThread
			Require: V = {all attributes}, E={}
			Begin [Drafting]
				NewThread(MR_MI)
				I(X,Y)=Calculate_MI(X,Y)
				Let L = {X,Y|I(X,Y) > epsilon),X,Y element of V, X!= Y}
				Sort L into decreasing order
				For each <X,Y> in L:
				If there is no adjacency path between X and Y in current graph E (Depth first search?)
					add <X,Y> to E and remove <X,Y> from L
			Begin [Thickening]
				For each <X,Y> in L:
					If EdgeNeeded( (V,E),X,Y) )
						Add <X,Y> to E
			Begin [Thinning]
			For each <X,Y> in E:
				If there are other paths, besides this arc, connecting X and Y,
					E' = E - <X,Y>
				If not EdgeNeeded( (V,E),X,Y) )
					E = E'
			Return[ OrientEdges(V,E) ]
			
			EdgeNeeded
			Require <X,Y>, V={all attributes}, E={current edge list}
			D(<X,Y>)=MinimumCutSet(<X,Y>,E)
			NewThread(MR_CMI(<X,Y>,D(<X,Y>)))
			I(X,Y|D(<X,Y>)) = Calculate_CMI(<X,Y>,D(<X,Y>)))
			If I(X,Y|D(<X,Y>)) < epsilon
				return false
			else
				return true
				
			MR_MI::Map
			Require: a line of training dataset L, L element of D
			for i < L.length
				Output(L[i],one);
			for i < L.length
				for j=i+1 and j < L.length
					Output(L[i]+L[j],one);
			
			MR_CMI::Map
			Require: a line of training dataset L, L element of D, two attributes v1, v2, condition-set V
			value1=L.extract(v1);
			value2=L.extract(v2);
			value3=L.extract(V);
			Output(value1+value3,one)
			Output(value2+value3,one)
			Output(value1+value2+value3,one);
			Output(value3,one);
			
			MR_MI::Reduce
			Require: Key, Value_List
			total=sum(value_list)
			Output(key,total)
			
			Calculate_MI:
			I(A,B) = Sum_{a,b}P(a,b)log({P(a,b)}/{P(a)P(b)})
			
			Calculate_CMI:
			I(A,B|C) = Sum_{a,b,c} P(a,b|c)log({P(a,b|C}/{P(a|c)P(b|c)})
			
			OrientEdges
			Require: V,E
			??
		
			MinimumCutSet
			Require: <X,Y>,E
			??
			
			AdjacencyPath
			??
			
		*/
		//GAME PLAN:
		//1. we need code for count calculation (is there but needs tweaking)
		calculateCounts(conf);
		//2. we need code for count processing into intermediate(to be created)
		processCounts(conf);
		//3. we need code to take intermediate so we have all datapoints for the G2 calculation
		return 0;
	}

	void calculateCounts(Configuration conf) throws Exception {
		//init job
		Job job = new Job(conf);
		job.setJobName("Distributed Independence Test - Calculate Counts");
		job.setJarByClass(ChenIndependenceJob.class);
		job.setMapperClass(ChenIndCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ChenIndCounterReducer.class);
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
		job.setJarByClass(ChenIndependenceJob.class);
		job.setMapperClass(ChenIndCountProcMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ChenIndCountProcReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);

		//Set input and output paths
		Path inputPath = new Path(conf.get("countoutput"));
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path(conf.get("processedcounts"));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Run the job
		job.waitForCompletion(true);	
	}
		
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new ChenIndependenceJob(), args);
		System.exit(exitCode);
	}
}
