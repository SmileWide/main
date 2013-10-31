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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import smile.Network;
import smile.learning.BayesianSearch;
import smile.learning.DataSet;
import smile.wide.hadoop.io.StrucLogLikeWritable;

/**
 * Mapper class used for running SMILE's Bayesian Search algorithm.
 * Reads datafile from distributed cache, runs BayesianSearch class
 * Result is Bayesian network in String form and Structure score
 * @author m.a.dejongh@gmail.com
 *
 */
public class SMILEBSMapper extends Mapper<LongWritable, Void, Text, StrucLogLikeWritable> {
	
	/** contains name of data file used for training*/
	private String trainfile;
	/** SMILE dataset format*/
	private DataSet ds = new DataSet();
	/** key for mapper, is current a constant*/
	private static final Text mkey = new Text("smile-bayesian-search");
	/** variable to temporarily store result of mapper */
	private StrucLogLikeWritable result = new StrucLogLikeWritable();

	//Bayesian Search parameters
	/** Number of iterations the algorithm is run, after an iteration is finished a random restart occurs*/
	private int iterationCount;
	/** Probability of generating an arc between two nodes when performing the random restart*/
	private double linkProbability;
	/** Limits the number of parents a node can have*/
	private int maxParents;
	/** Limits the amount of time the algorithm can spend on searching for a network structure (0 is unlimited)*/
	private int maxSearchTime;
	/** Influences prior probability of network structures during Bayesian Search*/
	private double priorLinkProbability;
	/** Equivalent sample size for Bayesian Search, larger values increase parameter resistance to data*/
	private int priorSampleSize;
	/** Random seed used for the random restart procedure*/
	private int randSeed;

	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		trainfile = conf.get("trainfile");
		iterationCount = conf.getInt("iterationCount",20);
		linkProbability = conf.getFloat("linkProbability", 0.01f);
		maxParents = conf.getInt("maxParents",8);
		maxSearchTime = conf.getInt("maxSearchTime",0);
		priorLinkProbability = conf.getFloat("priorLinkProbability", 0.001f);
		priorSampleSize = conf.getInt("priorSampleSize", 50);
		randSeed = 0;
		ds.readFile(trainfile);
	}
	
	/**Mapper: extracts random seed from input, initializes SMILE BayesianSearch algorithm with
	 * seed, runs BayesianSearch, stores network and Bayesian score to be sent to reducer.
	 */
	@Override
	protected void map(LongWritable key, Void value, Context context) throws IOException, InterruptedException {
		System.err.println("Running map code");
		//get seeds
		long k = key.get();
		randSeed = (int)(k & 0xffffffffL);
		double score = Double.NEGATIVE_INFINITY;

		BayesianSearch bs = new BayesianSearch();
		//Initialize the BS parameters
		bs.setRandSeed(randSeed);
		bs.setIterationCount(iterationCount);
		bs.setPriorSampleSize(priorSampleSize);
		bs.setLinkProbability(linkProbability);
		bs.setPriorLinkProbability(priorLinkProbability);
		bs.setMaxSearchTime(maxSearchTime);
		bs.setMaxParents(maxParents);

		//Do Bayesian search
		Network bnet =  bs.learn(ds);
		//Save loglikelihood and the network
		score = bs.getLastScore();
		result.setLogLike(score);
		result.setNW(bnet.writeString());//double check the string command

		context.write(mkey, result);
		//cleanup
		bnet.dispose();
		bs.dispose();
	}
}
