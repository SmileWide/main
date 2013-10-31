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

import smile.wide.Network;
import smile.wide.data.DataSet;
import smile.wide.data.SMILEData;


/** A wrapper for SMILE's BayesianSearch algorithm
 * Class is used for running SMILE's Bayesian Search algorithm
 * locally.
 * @author m.a.dejongh@gmail.com
 */
public class BayesianSearchSMILE extends BayesianSearch {
	/** Instance of SMILE's BayesianSearch algorithm*/
	smile.learning.BayesianSearch alg = null;
	/** Number of iterations the algorithm is run, after an iteration is finished a random restart occurs*/
	int iterationCount;
	/** Probability of generating an arc between two nodes when performing the random restart*/
	double linkProbability;
	/** Limits the number of parents a node can have*/
	int maxParents;
	/** Limits the amount of time the algorithm can spend on searching for a network structure (0 is unlimited)*/
	int maxSearchTime;
	/** Influences prior probability of network structures during Bayesian Search*/
	double priorLinkProbability;
	/** Equivalent sample size for Bayesian Search, larger values increase parameter resistance to data*/
	int priorSampleSize;
	/** Random seed used for the random restart procedure*/
	int randSeed;
	
	/** Constructors, sets parameter fields to SMILE's Bayesian Search default values*/
	BayesianSearchSMILE() {
		alg = new smile.learning.BayesianSearch();
		iterationCount = alg.getIterationCount();
		linkProbability = alg.getLinkProbability();
		maxParents = alg.getMaxParents();
		maxSearchTime = alg.getMaxSearchTime();
		priorLinkProbability = alg.getPriorLinkProbability();
		priorSampleSize = alg.getPriorSampleSize();
		randSeed = alg.getRandSeed();
	}
	
	/** Learn function, takes general data set class downcasts to
	 * SMILE version (exception if it's not the right class).
	 * Calls SMILE's BayesianSearch algorithm and returns a Network object containing
	 * a SMILE Network object. 
	 * @param data general data set class to be used as input
	 * @return returns an instance of the Network class 
	 * that contains a Bayesian network.
	 */	
	@Override
	Network learnStructure(DataSet data) {
		//Downcasting to SMILE dataset version (assuming this throws exception if it fails)
		//Perhaps it's better to scrap SMILEData and have a getSMILEdata function in DataContainer that returns DataSet() (or throws exception when fails)
		SMILEData sdata = (SMILEData) data;
		//extracting the actual dataset;
		smile.learning.DataSet d = sdata.getData();
		//run Bayesian search (will need to allow for specification of parameters)
		smile.Network net = alg.learn(d);
		
		Network n = new Network(); //could add logger functionality to this class (and the rest of the hierarchy and pass it in here then)
		
		
		n.setNetwork(net.writeString());
		return n;
	}

	public int getIterationCount() {
		return iterationCount;
	}

	public void setIterationCount(int iterationCount) {
		this.iterationCount = iterationCount;
	}

	public double getLinkProbability() {
		return linkProbability;
	}

	public void setLinkProbability(double linkProbability) {
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

	public double getPriorLinkProbability() {
		return priorLinkProbability;
	}

	public void setPriorLinkProbability(double priorLinkProbability) {
		this.priorLinkProbability = priorLinkProbability;
	}

	public int getPriorSampleSize() {
		return priorSampleSize;
	}

	public void setPriorSampleSize(int priorSampleSize) {
		this.priorSampleSize = priorSampleSize;
	}

	public int getRandSeed() {
		return randSeed;
	}

	public void setRandSeed(int randSeed) {
		this.randSeed = randSeed;
	}
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		System.err.println("No Main function for this class");
		System.exit(0);
	}

}
