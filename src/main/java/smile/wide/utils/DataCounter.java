package smile.wide.utils;

import java.util.ArrayList;
import smile.wide.data.DataSet;

public abstract class DataCounter {
	/**dataset to base the tree on*/
	DataSet ds;

	/**returns number of variables*/
	abstract public int numVars();

	/**returns number of states for a variables
	 * 
	 * @param v variable
	 * @return number of states
	 */
	abstract public int numStates(int v);

	/**construtor
	 * initializes the AD tree
	 * using the dataset
	 * @param ds_ dataset
	 */
	public DataCounter(DataSet ds_) {
		ds = ds_;
	}

	/**returns the co-occurrence count of the provided variable assignment
	 * @param query, variable assignment
	 * */
	abstract public int getCount(ArrayList<Pair<Integer, Integer> > _query);
}
