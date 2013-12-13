package smile.wide.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ListIterator;

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
class LazyVaryNode//OK
{
	/**constructor*/
	LazyVaryNode() { adnodes = null;}
	/**array with adnodes*/
	ArrayList<LazyADNode> adnodes;
};

/** LazyVary node datastructure 
 * Represents an AD node
 * of an AD tree
 * @author m.a.dejongh@gmail.com
 */
class LazyADNode
{
	/**constructor, sets array to null and inits count to 0*/
	LazyADNode() { varynodes = null; count = 0; }
	/**constructor, sets array to null, inits count to 0, and sets max to m*/
    LazyADNode(int m) { varynodes = null; count = 0; max = m;}
    /** count for current variable assignment*/
    int count;
    /** max number of children*/
	int max;
	ArrayList<LazyVaryNode> varynodes;
};

/**ADTree class
 * the class is "lazy" since entries
 * are only added to the tree when 
 * they are necessary
 * @author m.a.dejongh@gmail.com
 */
public class LazyADTree extends DataCounter{
	/**number of variables*/
	int nvar;
	/**arry with number of states
	 * for each variable
	 */
	ArrayList<Integer> nstates;
	/**root node of the AD tree*/
	LazyADNode root;

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
		create();
	}

	/**clears the tree*/
	void kill()
	{
		root.varynodes.clear();
		root = null;
	}

	/**creates a new tree*/
	void create()
	{
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
	}
	
	/**returns the co-occurrence count of the provided variable assignment
	 * @param query, variable assignment
	 * */
	public int getCount(ArrayList<Pair<Integer, Integer> > _query)
	{
		boolean dosort = false;
		ArrayList<Pair<Integer, Integer> > query = new ArrayList<Pair<Integer,Integer>>();
    	for(Pair<Integer,Integer> i : _query) {
    	    query.add(new Pair<Integer,Integer>(i.getFirst(),i.getSecond()));
    	}
    	try {
			if (query.size() == 0) {
				return root.count;
			}
			for(int x = 0; x < query.size()-1; ++x)
				if(query.get(x).getFirst() > query.get(x+1).getFirst()) {
					dosort = true;
					break;
				}
			if(dosort)
				Collections.sort(query, new PairComparator());

			MutableInt idx = new MutableInt(query.size() - 1);
			int count = retrieveCount(root, query, idx);
			if (count >= 0) {
				return count;
			}
			else {
				updateTreeCounts(root, query, idx.intValue());
				idx = new MutableInt(query.size() - 1);
				count = retrieveCount(root, query, idx);
				assert(count >= 0);
				return count;
			}
		}
		catch (OutOfMemoryError e) {
			kill();
			create();
			return getCount(query);
		}
	}

	/**function that traverses the tree to get the count
	 * @param current AD node
	 * @param query, variable assignment
	 * @param node index of variable assignment
	 * */
	int retrieveCount(LazyADNode ad, ArrayList<Pair<Integer, Integer> > query, MutableInt idx) {
		while (idx.intValue() >= 0) {
			if (ad.varynodes == null) {
				return -1;
			}
			else {
				Pair<Integer, Integer> p = query.get(idx.intValue());
				LazyVaryNode v = ad.varynodes.get(p.getFirst());
				if(v==null) {
					return -1;
				}
				if (v.adnodes == null) {
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
	void updateTreeCounts(LazyADNode root, ArrayList<Pair<Integer, Integer> > query, int idx) {
		LazyADNode tmpad;
		ListIterator<Pair<Integer,Integer>> tmpq;
	    int qsize = query.size();
		int recCount = ds.getNumberOfRecords();
	    for (int i = 0; i < recCount; i++)
	    {
			tmpad = root;
			tmpq = query.listIterator(qsize);
	        for (int j = qsize - 1; j >= 0; j--) {
				// select variable and its value
				int var = tmpq.previous().getFirst();//and move back
	            int val = ds.getInt(var, i);
				// create varynode and adnode if necessary
				int max = tmpad.max;
				if (tmpad.varynodes == null) {
					tmpad.varynodes = new ArrayList<LazyVaryNode>(Collections.nCopies(max, (LazyVaryNode) null));
				}
				if(tmpad.varynodes.get(var) == null) {
					tmpad.varynodes.set(var, new LazyVaryNode());
				}
				LazyVaryNode v = tmpad.varynodes.get(var);//pointer arithmetic 
				if (v.adnodes == null) {
					v.adnodes = new ArrayList<LazyADNode>(Collections.nCopies(nstates.get(var), (LazyADNode) null));
				}
				if (v.adnodes.get(val) == null) {
					v.adnodes.set(val,new LazyADNode(var));
				}
				tmpad = v.adnodes.get(val);
				// only increment count if this part of the tree is new
				if (j <= idx) {
					tmpad.count++;
				}
	        }
	    }
	}
}
