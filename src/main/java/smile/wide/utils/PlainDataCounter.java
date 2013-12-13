package smile.wide.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import smile.wide.data.DataSet;

public class PlainDataCounter extends DataCounter {

	HashMap<ArrayList<Pair<Integer, Integer >>,Integer> cache = new HashMap<ArrayList<Pair<Integer, Integer >>,Integer>();
	
	public PlainDataCounter(DataSet ds_) {
		super(ds_);
	}

	/**returns number of variables*/
	public int numVars() {
		return ds.getNumberOfVariables();
	}

	/**returns number of states for a variables
	 * 
	 * @param v variable
	 * @return number of states
	 */
	public int numStates(int v) {
		return ds.getStateNames(v).length;
	}

	/**returns the co-occurrence count of the provided variable assignment
	 * @param query, variable assignment
	 * */
	public int getCount(ArrayList<Pair<Integer, Integer> > _query) {
		//Empty queries can be done quicker
		if(_query.isEmpty())
			return ds.getNumberOfRecords();
		//sort query if necessary
		boolean dosort = false;
		ArrayList<Pair<Integer, Integer> > query = new ArrayList<Pair<Integer,Integer>>();
    	for(Pair<Integer,Integer> i : _query) {
    	    query.add(new Pair<Integer,Integer>(i.getFirst(),i.getSecond()));
    	}
		for(int x = 0; x < query.size()-1; ++x)
			if(query.get(x).getFirst() > query.get(x+1).getFirst()) {
				dosort = true;
				break;
			}
		if(dosort)
			Collections.sort(query, new PairComparator());

		//We cache queries for if they are used again.
		if(cache.containsKey(query))
			return cache.get(query);
		int count = 0;
		for(int x=0;x<ds.getNumberOfRecords();++x) {
			boolean match = true;
			for(Pair<Integer,Integer> assignment : query) {
				int var = assignment.getFirst();
				int val = assignment.getSecond();
				if(ds.getInt(var, x) != val) {
					match = false;
					break;
				}
			}
			if(match)
				count++;
		}
		cache.put(query, count);
		return count;
	}
}
