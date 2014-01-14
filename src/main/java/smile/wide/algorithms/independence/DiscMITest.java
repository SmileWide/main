package smile.wide.algorithms.independence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.mutable.MutableDouble;

import smile.wide.data.DataSet;
import smile.wide.utils.DataCounter;
import smile.wide.utils.LazyADTree;
import smile.wide.utils.Pair;

//CURRENTLY DOESN'T WORK, MI needs to be compared differently than p-values

/** Independence test for continuous data 
 * (Assumes Gaussian distributions)
 * Is initialized with a DataSet
 * calculates p-value of test
 * @author m.a.dejongh@gmail.com
 */
public class DiscMITest extends IndependenceTest {
	/**AD Tree datastructure, contains counts*/
	DataCounter ad = null;
	/**Constructor
	 * initializes AD Tree structure
	 * @param tds
	 */
	public DiscMITest(DataSet tds, DataCounter dc) {
		super(tds);
		if(dc == null)
			ad = new LazyADTree(ds);
		else
			ad = dc;
	}
	/**calcPValue
	 * here calculates MI value
	 * @param x variable
	 * @param y variable
	 * @param z list with conditioning variables	
	 */
	@Override
	public double calcPValue(int x, int y, ArrayList<Integer> z, MutableDouble mi) {
		int i;
        int xpos = -1;
        int ypos = -1;
        ArrayList<Integer> vars = new ArrayList<Integer>();
        ArrayList<Integer> states = new ArrayList<Integer>();
        ArrayList<Integer> nstates = new ArrayList<Integer>();

        // make sure the variables are ordered in their index
        // keep the position of x and y
        int prev = -1;
        for (i = 0; i < z.size(); i++)
        {
            int tmpz = z.get(i);
            if (x < y)
            {
                if (x < tmpz && (prev == -1 || x > prev))
                {
                    xpos = vars.size();
                    vars.add(x);
                    states.add(0);
                    nstates.add(ad.numStates(x));
                }
                if (y < tmpz && (prev == -1 || y > prev))
                {
                    ypos = vars.size();
                    vars.add(y);
                    states.add(0);
                    nstates.add(ad.numStates(y));
                }
            }
            else
            {
                if (y < tmpz && (prev == -1 || y > prev))
                {
                    ypos = vars.size();
                    vars.add(y);
                    states.add(0);
                    nstates.add(ad.numStates(y));
                }
                if (x < tmpz && (prev == -1 || x > prev))
                {
                    xpos = vars.size();
                    vars.add(x);
                    states.add(0);
                    nstates.add(ad.numStates(x));
                }
            }
            vars.add(tmpz);
            states.add(0);
            nstates.add(ad.numStates(tmpz));
            prev = tmpz;
        }

        // x was not inserted
        if (xpos == -1)
        {
            xpos = vars.size();
            vars.add(x);
            states.add(0);
            nstates.add(ad.numStates(x));
        }

        // y was not inserted
        if (ypos == -1)
        {
            ypos = vars.size();
            vars.add(y);
            states.add(0);
            nstates.add(ad.numStates(y));
        }

        // calculate the number of configurations
        int nconf = 1;
        for (i = 0; i < (int) nstates.size(); i++)
        {
            nconf *= nstates.get(i);
        }
        //check if 10 * nconf < nsamples (Spirtes & Glymour)
        if(10*nconf >= ds.getNumberOfRecords()) {
        	return -1.0;
        }
        // main loop
        int nvars = vars.size();
        double g2 = 0;
    	Set<ArrayList<Pair<Integer,Integer> > > test = new HashSet<ArrayList<Pair<Integer,Integer> > >();
        for (i = 0; i < nconf; i++)
        {
            if (i != 0)
            {
                boolean done = false;
                for (int j = 0; j < (int) states.size() && !done; j++)
                {
                    done = true;
                    states.set(j, states.get(j)+1);
                    if (states.get(j) == nstates.get(j))
                    {
                    	states.set(j,0);
                        done = false;
                    }
                }
            }
            ArrayList<Pair<Integer, Integer> > tmp = new ArrayList<Pair<Integer, Integer> >();
            int j;
            for (j = 0; j < nvars; j++)
            {
            	tmp.add(new Pair<Integer,Integer>());
                tmp.get(j).setFirst(new Integer(vars.get(j)));
                tmp.get(j).setSecond(new Integer(states.get(j)));
            }
            int xijk = ad.getCount(tmp);
            int idx = 0;
            tmp.subList((nvars-1),tmp.size()).clear();
            for (j = 0; j < nvars; j++)
            {
                if (j == ypos)
                {
                    continue;
                }
                Pair<Integer, Integer> p = tmp.get(idx);
                p.setFirst(vars.get(j));
                p.setSecond(states.get(j));
                idx++;
            }
            int xi_k = ad.getCount(tmp);
    		if(xi_k == 0 ) {
    				ArrayList<Pair<Integer,Integer>> templst = new ArrayList<Pair<Integer,Integer>>();
    				for(Pair<Integer,Integer> p: tmp)
    					templst.add(new Pair<Integer,Integer>(p.getFirst(),p.getSecond()));
    				test.add(templst);
    		}
            idx = 0;
            for (j = 0; j < nvars; j++)
            {
                if (j == xpos)
                {
                    continue;
                }
                Pair<Integer, Integer> p = tmp.get(idx);
                p.setFirst(vars.get(j));
                p.setSecond(states.get(j));
                idx++;
            }
            int x_jk = ad.getCount(tmp);
    		if(x_jk == 0)
    		{
    				ArrayList<Pair<Integer,Integer>> templst = new ArrayList<Pair<Integer,Integer>>();
    				for(Pair<Integer,Integer> p: tmp)
    					templst.add(new Pair<Integer,Integer>(p.getFirst(),p.getSecond()));
    				test.add(templst);
			}
            idx = 0;
            tmp.subList((nvars-2),tmp.size()).clear();
            
            for (j = 0; j < nvars; j++)
            {
                if (j == xpos || j == ypos)
                {
                    continue;
                }
                Pair<Integer, Integer> p = tmp.get(idx);
                p.setFirst(vars.get(j));
                p.setSecond(states.get(j));
                idx++;
            }
            int x__k = ad.getCount(tmp);
            if (xijk > 0)
            {
                double logexijk = Math.log((double) xi_k) + Math.log((double) x_jk) - Math.log((double) x__k);
                g2 += xijk * (Math.log((double) xijk) - logexijk);
            }
        }
        g2 *= 2;
        //Mutual Information calculation
        double m_inf = g2 / (2.0 * ds.getNumberOfRecords() * Math.log(2.0));
        mi.setValue(m_inf);
    	return m_inf;
	}

	protected boolean compareResult(double pval, double significance, double extreme_value) {
    	return pval < significance && pval < extreme_value;
    }
    protected double init_extreme_value() {
    	return Double.MAX_VALUE;
    }
    protected boolean better(double left, double right) {
    	return left < right;
    }
}
