package smile.wide.algorithms.independence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;

import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;

/** Abstract class for (conditional independence tests)
 * contains the basic functions to test for independence
 * such as iterating through all possible conditioning sets
 * but requires implementation of the actual test, be it
 * for continuous or discrete data.
 * @author m.a.dejongh@gmail.com
 */	
public abstract class IndependenceTest {
	/** dataset used for independence tests */
	protected DataSet ds;
	/** constructor
	 * @param tds dataset
	 */
	public IndependenceTest(DataSet tds) {
		ds = tds;
	}
	/** toplevel function for independence calculation
	 * 
	 * @param pat current state of the structure
	 * @param card maximum size of conditioning set
	 * @param x variable x
	 * @param y variable y
	 * @param sepset empty list to be filled with sepset of x and y
	 * @param signif significance level to be used for the test
	 * @return true: independent, false: not independent.
	 */
    public boolean findCI(Pattern pat, int card, int x, int y, HashSet<Integer> sepset, double signif)
    {
        MutableDouble pvalxy = new MutableDouble(0);
        HashSet<Integer> sepsetxy = new HashSet<Integer>();
        checkCI(pat, card, x, y, pvalxy, sepsetxy, signif);

        MutableDouble pvalyx = new MutableDouble(0);
        HashSet<Integer> sepsetyx = new HashSet<Integer>();
        checkCI(pat, card, y, x, pvalyx, sepsetyx, signif);
       
        if (pvalxy.doubleValue() > 0 || pvalyx.doubleValue() > 0)
        {
            if (pvalxy.doubleValue() > pvalyx.doubleValue())
            {
                for(Integer q: sepsetxy)
                	sepset.add(q);
            }
            else
            {
                for(Integer q: sepsetyx)
                	sepset.add(q);
            }
            return true;
        }
        else
        {
            return false;
        }
    }
    /** Lower level independence function, constructs
     * array to be used for testing all combinations of
     * conditional independence tests
     * @param pat current state of the structure
     * @param card maximum size of conditioning set
	 * @param x variable x
	 * @param y variable y
     * @param maxpval output variable for maximum p-value acquired from test
	 * @param sepset empty list to be filled with sepset of x and y
	 * @param signif significance level to be used for the test
     */
    protected void checkCI(Pattern pat, int card, int x, int y, MutableDouble maxpval, HashSet<Integer> sepset, double signif)
    {
        int nvar = ds.getNumberOfVariables();//number of variables in dataset

        // <x,y>
        assert(pat.getEdge(x, y) != Pattern.EdgeType.None);

        // populate elements vector
        ArrayList<Integer> elements=new ArrayList<Integer>();
        int i;
        for (i = 0; i < nvar; i++) {
            if (i != x && i != y && pat.getEdge(x, i) == Pattern.EdgeType.Undirected) {
                    elements.add(i);
            }
        }

        // check for enough elements
        if ((int) elements.size() < card) {
            return;
        }

        // generate conditioning sets
        ArrayList<Boolean> binvec = new ArrayList<Boolean>(Collections.nCopies(elements.size(), false));
        for (i = 0; i < card; i++) {
            binvec.set(i,true);
        }

        // test for conditional independence
        boolean first = true;
    	MutableInt cur = new MutableInt(card - 1);
    	MutableInt ones = new MutableInt(0);
        for (; first || NxtSubset(binvec, cur, ones); ) {
            first = false;
            ArrayList<Integer> z = new ArrayList<Integer>();
            for (i = 0; i < (int) binvec.size(); i++) {
                if (binvec.get(i)) {
                    z.add(elements.get(i));
                }
            }
            double pval;
            pval = calcPValue(x, y, z);
            if (pval > signif && pval > maxpval.doubleValue())
            {
                maxpval.setValue(pval);
                sepset.clear();
                for(Integer q : z) {
                	sepset.add(q.intValue());
                }
            }
        }
    }
    
    /** function for iterating through all possible subsets
     * @param binvec array that represents all subsets
     * @param cur pointer in array
     * @param ones counter
     * @return true, more subsets to consider, false we are done
     */
    private boolean NxtSubset(ArrayList<Boolean> binvec, MutableInt cur, MutableInt ones)
    {
    	if (cur.intValue() < 0)
    	{
    		return false;
    	}

    	int size = binvec.size();
    	if ((cur.intValue() + 1 < size) && !binvec.get(cur.intValue() + 1))
    	{
    		binvec.set(cur.intValue(),false);
    		cur.increment();
    		binvec.set(cur.intValue(),true);
    		return true;
    	}
    	else
    	{
    		ones.increment();
    		for (int i = cur.intValue() - 1; i >= 0; i--)
    		{
    			if (binvec.get(i))
    			{
    				if (!binvec.get(i + 1))
    				{
    					binvec.set(i,false);
    					binvec.set(i + 1,true);
    					int j;
    					for (j = i + 2; j < (i + ones.intValue() + 2); j++)
    					{
    						binvec.set(j,true);
    					}
    					cur.setValue(j - 1);
    					ones.setValue(0);
    					for (; j < size; j++)
    					{
    						binvec.set(j,false);
    					}
    					return true;
    				}
    				else
    				{
    					ones.increment();
    				}
    			}
    		}
    		return false;
    	}
    }
    /** p-value calculation function.
     * Effectively the function that calculates if
     * variables x and y are independent given z
     * @param x variable 
     * @param y variable 
     * @param z list of conditioning variables
     * @return p-value of independence test
     */
    protected abstract double calcPValue(int x, int y, ArrayList<Integer> z);
}
