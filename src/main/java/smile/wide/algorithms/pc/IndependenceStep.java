package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.Set;

import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public abstract class IndependenceStep {
	public abstract void execute(DataSet ds, Pattern pat, boolean disc, int maxAdjacency, double significance, ArrayList<ArrayList<Set<Integer> > > sepsets);
}
