package smile.wide.algorithms.labelpropagation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import smile.wide.utils.Pair;

public class LProp {

	//cluster algorithm
	public void CalcClusters(ArrayList<Integer> node_clusters, ArrayList<ArrayList<Double>> adjacency_matrix)
	{
		//int node_clusters array
		Integer nvar = adjacency_matrix.size();
		ArrayList<Integer> vs = new ArrayList<Integer>();
		node_clusters.clear();
		for(int x=0;x<nvar;++x) {
			node_clusters.add(x);
			vs.add(x);//vertices
		}
	  
		//Basic principle of algorithm is that all nodes are initialised
		//with a label (a number). The algorithm iterates over all nodes
		//Every node compares the labels of it's neighbours, picking the
		//most common label as it's new label (breaking ties randomly).
		boolean stable = false;
		//algorithm runs until label assignment converges
		long seed = System.nanoTime();//might need something better
		Random rnd = new Random(seed);
		
		while(!stable)
		{
			stable = true;
			Collections.shuffle(vs,rnd);
			for(Integer it : vs) {
				ArrayList<Integer> ad = new ArrayList<Integer>();
				adjacentVertices(ad, adjacency_matrix, it.intValue());
				HashMap<Integer,Double> freq = new HashMap<Integer,Double>();
				for(Integer a : ad)
				{
					double weight = adjacency_matrix.get(it).get(a);
					if(weight > 0.0) {
						Integer n_cluster = node_clusters.get(a);
						if(!freq.containsKey(n_cluster) )
							freq.put(n_cluster, weight);
						else
							freq.put(n_cluster, freq.get(n_cluster)+weight);
					}
				}
	    		if(!freq.isEmpty())//if node has neighbours
	    		{
	    			ArrayList<Pair<Integer,Double> > freqq = new ArrayList<Pair<Integer,Double>>();
	    			//fill freqq
	    		    Iterator itt = freq.entrySet().iterator();
	    		    while (itt.hasNext()) {
	    		        Map.Entry<Integer,Double> pairs = (Map.Entry<Integer,Double>) itt.next();
	    		        freqq.add(new Pair<Integer,Double>(pairs.getKey(),pairs.getValue()));
	    		    }
	    			Collections.sort(freqq, new revvecComparator());
	    			//random tie breaking for cluster with same weight
	    			for(int x=0;x<freqq.size()-1;++x) {
	    				Integer y = x+1;
	    				if(freqq.get(x).getSecond() == freqq.get(y).getSecond()) {
	    					if(rnd.nextDouble() < 0.5)
	    						Collections.swap(freqq, x, y);
	    				}
	    			}
	    			//get the "biggest" cluster
	    			Integer new_cluster = freqq.get(0).getFirst();
	    			if(new_cluster != node_clusters.get(it)) {
	    				stable = false;
	    				node_clusters.set(it, new_cluster);
	    			}
               }	
	    	}
		}
	}
	
	void adjacentVertices(ArrayList<Integer> adj, ArrayList<ArrayList<Double>> adjacency_matrix, Integer i) {
		adj.clear();
		for(int x=0;x<adjacency_matrix.size();++x) {
			if(adjacency_matrix.get(i).get(x) > 0.0)
				adj.add(x);
		}
	}

	// edge comparison function, based on edge weight
	class revvecComparator implements Comparator<Pair<Integer,Double>> {
		/**compare function*/
		@Override
		public int compare(Pair<Integer, Double> arg0, Pair<Integer,Double> arg1) {
			return -1 * arg0.getSecond().compareTo(arg1.getSecond());
		}
	}	

}