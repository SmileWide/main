/**
 * 
 */
package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import smile.wide.algorithms.independence.ContIndependenceTest;
import smile.wide.algorithms.independence.DiscIndependenceTest;
import smile.wide.algorithms.independence.IndependenceTest;
import smile.wide.data.DataSet;
import smile.wide.utils.DataCounter;
import smile.wide.utils.LazyADTree;
import smile.wide.utils.Pair;
import smile.wide.utils.Pattern;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public class RandomIndependenceStep extends IndependenceStep {

	int seed = 0;
	int numberoftests = 0;
	public ArrayList<Pair<Integer,Integer>> removed = null;

	public RandomIndependenceStep(int s, int n) {
		seed = s;
		numberoftests = n;
	}
	
	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int adjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) {
        IndependenceTest itest = null;
        DataCounter ct = null;
//        if(adjacency > 3)//TODO MDJ: need this?
//        	ct = new LazyADTree(ds,10);
        if(disc)
        	itest = new DiscIndependenceTest(ds, ct);
        else
        	itest = new ContIndependenceTest(ds);

        //randomly generate x, randomly generate y, and if edges needs to be done, test it.
        //perhaps add an extra container for the removed edges
        Random rn = new Random(seed);
        
        //count number of edges in pattern
        int edgesLeft = 0;
        for(int x = 0; x < pat.getSize(); ++x)
        	for(int y=x+1;y< pat.getSize(); ++y)
        		if(pat.getEdge(x,y) != Pattern.EdgeType.None) {
        			edgesLeft++;
        		}
        
        HashSet<Pair<Integer,Integer>> examined = new HashSet<Pair<Integer,Integer>>();
        if(numberoftests > edgesLeft)
        	numberoftests = edgesLeft;

        // find conditional independencies        
        while (numberoftests > 0)
        {
        	int x = rn.nextInt(pat.getSize()-1);
        	int y = x+1+rn.nextInt(pat.getSize()-x-1);
        	if (!examined.contains(new Pair<Integer,Integer>(x,y)) && !(pat.getEdge(x, y) == Pattern.EdgeType.None && pat.getEdge(y, x) == Pattern.EdgeType.None)) {
            	//do test
        		examined.add(new Pair<Integer,Integer>(x,y));
	            HashSet<Integer> sepset= new HashSet<Integer>();
	            if (itest.findCI(pat, adjacency, x, y, sepset, significance))
	            {
	                pat.setEdge(x, y, Pattern.EdgeType.None);
	                pat.setEdge(y, x, Pattern.EdgeType.None);
	                sepsets.get(x).set(y,sepset);
	                sepsets.get(y).set(x,sepset);
	                removed.add(new Pair<Integer,Integer>(x,y));
	            }
            	numberoftests--;
            }
        }
	}
}
