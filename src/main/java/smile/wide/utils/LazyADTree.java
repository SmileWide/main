package smile.wide.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Stack;

import org.apache.commons.lang.mutable.MutableInt;

import smile.wide.data.DataSet;

/** Comparator class for Pair objects 
 * allows for sorting of Pairs by
 * their first element
 * @author m.a.dejongh@gmail.com
 */
class PairComparator implements Comparator<Pair<Integer,Integer>> {
	/**compare function*/
	@Override
	public int compare(Pair<Integer, Integer> arg0, Pair<Integer, Integer> arg1) {
		return arg0.getFirst().compareTo(arg1.getFirst());
	}
}

/** LazyVary node datastructure 
 * Represents a vary node
 * of an AD tree
 * @author m.a.dejongh@gmail.com
 */
class LazyVaryNode
{
	/**array with adnodes*/
	ArrayList<LazyADNode> adnodes = new ArrayList<LazyADNode>();
};

/** LazyVary node datastructure 
 * Represents an AD node
 * of an AD tree
 * @author m.a.dejongh@gmail.com
 */
class LazyADNode
{
	/**constructor, sets array to null and inits count to 0*/
	LazyADNode() {}
	/**constructor, sets array to null, inits count to 0, and sets max to m*/
    LazyADNode(int m) {max = m;}
    /** count for current variable assignment*/
    int count=0;
    /** max number of children*/
	int max=-1;
	boolean leaf_node = false;
	ArrayList<LazyVaryNode> varynodes = new ArrayList<LazyVaryNode>();
	ArrayList<Integer> record_indices = null;
};

/**ADTree class
 * the class is "lazy" since entries
 * are only added to the tree when 
 * they are necessary
 * @author m.a.dejongh@gmail.com
 */
public class LazyADTree extends DataCounter{
	int max_count = 0;
	int min_count = 0;
	int nvar;
	int free_adnodes = 10000;
	int free_varynodes = 10000;
	int adnodeptr = 0;
	int vrnodeptr = 0;
	LazyADNode root;
	ArrayList<Integer> nstates;
	ArrayList<LazyADNode> adnodes = new ArrayList<LazyADNode>();
	ArrayList<LazyVaryNode> varynodes = new ArrayList<LazyVaryNode>();
	Stack<LazyADNode> adnodebin = new Stack<LazyADNode>();
	Stack<LazyVaryNode> varynodebin = new Stack<LazyVaryNode>();
	ArrayList<Pair<Integer, Integer> > query = new ArrayList<Pair<Integer,Integer>>();
	PairComparator pcomp = new PairComparator();
	MutableInt mutint = new MutableInt();
	Runtime runtime = Runtime.getRuntime();
	
	private void wipeVaryNode(LazyVaryNode n) {
		if(n != null) {
			varynodebin.push(n);
			if(!n.adnodes.isEmpty()) {
				for(LazyADNode a: n.adnodes)
					wipeADNode(a);
				n.adnodes.clear();
			}
		}
	}
	
	private void wipeADNode(LazyADNode n) {
		if(n != null) {
			adnodebin.push(n);
			n.record_indices.clear();
			n.count = 0;
			n.leaf_node = false;
			if(!n.varynodes.isEmpty()) {
				for(LazyVaryNode v: n.varynodes)
					wipeVaryNode(v);
				n.varynodes.clear();
			}
		}
	}
	
	private void addADNodes(int n) {
		for(int x=0;x<n;++x)
			adnodes.add(new LazyADNode());
	}
	
	private void addVaryNodes(int n) {
		for(int x=0;x<n;++x)
			varynodes.add(new LazyVaryNode());
	}
	
	private LazyADNode newADNode(int var) {
		LazyADNode a = null;
		if(!adnodebin.isEmpty())
			a = adnodebin.pop();
		else {
			if(adnodeptr==free_adnodes) {
				addADNodes(10000);
				free_adnodes+=10000;
			}
			a = adnodes.get(adnodeptr++);
		}
		if(min_count > 0)
			a.record_indices = new ArrayList<Integer>();
		a.max = var;
		return a;
	}
	
	private LazyVaryNode newVaryNode() {
		LazyVaryNode v = null;
		if(!varynodebin.isEmpty()) {
			v = varynodebin.pop();
		}
		else {
			if(vrnodeptr==free_varynodes) {
				addVaryNodes(10000);
				free_varynodes +=10000;
			}
			v= varynodes.get(vrnodeptr++);
		}
		return v;
	}
	
	/**returns number of variables*/
	public int numVars() { 
		return nvar; 
	}
	/**returns number of states for a variables
	 * 
	 * @param v variable
	 * @return number of states
	 */
	public int numStates(int v) {
		return nstates.get(v);
	}

	/**construtor
	 * initializes the AD tree
	 * using the dataset
	 * @param ds_ dataset
	 */
	public LazyADTree(DataSet ds_)
	{
		super(ds_);
		create(min_count);
		addADNodes(free_adnodes);
		addVaryNodes(free_varynodes);
		
	}

	public LazyADTree(DataSet ds_,int mc)
	{
		super(ds_);
		create(mc);
	}

	
	/**clears the tree*/
	void kill()
	{
		root.varynodes.clear();
		adnodes.clear();
		varynodes.clear();
		adnodebin.clear();
		varynodebin.clear();	
		adnodeptr = 0;
		vrnodeptr = 0;
		free_adnodes = 10000;
		free_varynodes = 10000;
		root = null;
	}

	/**creates a new tree*/
	void create(int mc)
	{
		min_count = mc;
		max_count = mc;
		nvar = ds.getNumberOfVariables();
		nstates = new ArrayList<Integer>();
	    for (int j = 0; j < nvar; j++)
	    {
	        int nstate = (int) ds.getStateNames(j).length;
	        if (nstate == 0)
	        {
	            System.out.println("adtree: number of states is 0");
	            System.exit(-1);
	        }
	        nstates.add(new Integer(nstate));
	    }
	    root = new LazyADNode(nvar);
		root.count = ds.getNumberOfRecords();
		addADNodes(free_adnodes);
		addVaryNodes(free_varynodes);
	}
	
	/**returns the co-occurrence count of the provided variable assignment
	 * @param query, variable assignment
	 * */
	public int getCount(ArrayList<Pair<Integer, Integer> > _query)
	{
		boolean dosort = false;

		query.clear();
		for(Pair<Integer,Integer> i : _query)
			query.add(new Pair<Integer,Integer>(i.getFirst(),i.getSecond()));
		
		try {
			if (query.size() == 0)
				return root.count;
			for(int x = 0; x < query.size()-1; ++x)
				if(query.get(x).getFirst() > query.get(x+1).getFirst()) {
					dosort = true;
					break;
				}
			if(dosort)
				Collections.sort(query, pcomp);

			MutableInt idx = mutint;
			idx.setValue(query.size() - 1);
			int count = retrieveCount(root, query, idx);
			if (count >= 0) {
				return count;
			}
			else {
				updateTreeCounts(root, query, idx.intValue());
				idx.setValue(query.size() - 1);
				count = retrieveCount(root, query, idx);
				assert(count >= 0);

				//temp
		        double mm =  runtime.maxMemory();
		        double tm = runtime.totalMemory();
		        double fm = runtime.freeMemory();
		        double usage = (tm-fm)/mm;
		        if(usage > 0.75) {
					kill();
					create(min_count);
		        }
				return count;
			}
		}
		catch (OutOfMemoryError e) {
			kill();
			create(min_count);
			return getCount(query);
		}
	}

	/**function that traverses the tree to get the count
	 * @param current AD node
	 * @param query, variable assignment
	 * @param node index of variable assignment
	 * */
	int retrieveCount(LazyADNode ad, ArrayList<Pair<Integer, Integer> > _query, MutableInt idx) {
		while (idx.intValue() >= 0) {
			//LEAF NODE CODE *************************************
			if(ad.record_indices != null && !ad.record_indices.isEmpty() && ad.count > min_count) {
					ad.record_indices.clear();
					ad.record_indices = null;
			}
			if(ad.count > max_count) {
				ad.leaf_node = false;
		}
			if(!ad.leaf_node && ad.count > 0 && ad.count <= max_count) {
				ad.leaf_node = true;
				if(!ad.varynodes.isEmpty()) {
					for(LazyVaryNode v: ad.varynodes) {
						wipeVaryNode(v);
					}
					ad.varynodes.clear();
				}
			}
			if(ad.leaf_node) {
				if(ad.count <= min_count && ad.record_indices != null)
					return leafCount(_query,ad.record_indices);
				else
					return basicCount(_query);
			}
			//END LEAF NODE CODE *********************************
			if (ad.varynodes.isEmpty()) {
				return -1;
			}
			else {
				Pair<Integer, Integer> p = _query.get(idx.intValue());
				LazyVaryNode v = ad.varynodes.get(p.getFirst());
				if(v==null) { //And we did calculate all others? For MCV?
					return -1;
				}
				if (v.adnodes.isEmpty()) {
					return -1;
				}
				else {
					ad = v.adnodes.get(p.getSecond());
					if (ad == null) {
						return 0;
					}
					else {
						idx.decrement();
					}
				}
			}
		}
		return ad.count;
	}

	/** if counts a not yet present in the tree
	 * 
	 * @param root current root node
	 * @param query variable assignment
	 * @param idx node index of variable assignment
	 */
	void updateTreeCounts(LazyADNode root, ArrayList<Pair<Integer, Integer> > _query, int idx) {
		LazyADNode tmpad;
	    int qsize = _query.size();
		int recCount = ds.getNumberOfRecords();
	    for (int i = 0; i < recCount; i++)
	    {
			tmpad = root;
	        for (int j = qsize - 1; j >= 0; j--) {
				// select variable and its value
				int var = _query.get(j).getFirst();
	            int val = ds.getInt(var, i);
				// create varynode and adnode if necessary
				int max = tmpad.max;
				if (tmpad.varynodes.isEmpty()) {
					for(int x=0;x<max;++x)
						tmpad.varynodes.add(null);
				}
				if(tmpad.varynodes.get(var) == null) {
					tmpad.varynodes.set(var, newVaryNode());
				}
				LazyVaryNode v = tmpad.varynodes.get(var);//pointer arithmetic 
				if (v.adnodes.isEmpty()) {
					for(int x=0;x<nstates.get(var);++x)
						v.adnodes.add(null);
				}
				if (v.adnodes.get(val) == null) {
					v.adnodes.set(val,newADNode(var));
				}
				tmpad = v.adnodes.get(val);
				// only increment count if this part of the tree is new
				if (j <= idx) {
					tmpad.count++;
					//LEAF NODE CODE *************************************
					if(tmpad.count <= min_count && tmpad.record_indices != null)
						tmpad.record_indices.add(i);
					//END LEAF NODE CODE *********************************
				}
	        }
	    }
	}
	
	/** function for counting using leaf nodes
	 * idea is that nodes with small counts don't
	 * have kids, we recalculate when we need it
	 * @param query
	 * @param indices
	 * @return count
	 */
	int leafCount(ArrayList<Pair<Integer, Integer> > _query, ArrayList<Integer> indices) {
		int count = 0;
		for(Integer i : indices) {
			boolean cnt = true;
			for(Pair<Integer,Integer> p : _query) {
				int var = p.getFirst();
				int val = p.getSecond();
				if(val != ds.getInt(var, i)) {
					cnt = false;
					break;
				}
			}
			if(cnt)
				count++;
		}
		return count;
	}
	
	/** function for counting using the dataset
	 * @param query
	 * @return count
	 */
	int basicCount(ArrayList<Pair<Integer, Integer> > _query) {
		int count = 0;
		for(int  i=0;i<root.count;++i) {
			boolean cnt = true;
			for(Pair<Integer,Integer> p : _query) {
				int var = p.getFirst();
				int val = p.getSecond();
				if(val != ds.getInt(var, i)) {
					cnt = false;
					break;
				}
			}
			if(cnt)
				count++;
		}
		return count;
	}

}
