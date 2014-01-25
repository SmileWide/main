/**
 * 
 */
package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableDouble;

import smile.wide.algorithms.independence.ContIndependenceTest;
import smile.wide.algorithms.independence.DiscIndependenceTest;
import smile.wide.algorithms.independence.IndependenceTest;
import smile.wide.data.DataSet;
import smile.wide.utils.DataCounter;
import smile.wide.utils.Pair;
import smile.wide.utils.Pattern;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public class PartitionIndependenceStep extends IndependenceStep {

	int start = 0;
	int numberoftests = 0;
	public ArrayList<Pair<Integer,Integer>> removed = null;

	public PartitionIndependenceStep(int s, int n) {
		start = s;
		numberoftests = start+n;
	}
	
	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int adjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) {
        IndependenceTest itest = null;
        if(disc)
        	itest = new DiscIndependenceTest(ds, null);
        else
        	itest = new ContIndependenceTest(ds);

        //save coordinates in array for easy acces from starting point
        ArrayList<Integer> xcoord = new ArrayList<Integer>();
        ArrayList<Integer> ycoord = new ArrayList<Integer>();
        for(int x = 0; x < pat.getSize(); ++x)
        	for(int y=x+1;y< pat.getSize(); ++y)
        		if(pat.getEdge(x,y) != Pattern.EdgeType.None) {
        			xcoord.add(x);
        			ycoord.add(y);
        		}

        // find conditional independencies        
        for (int i=start;i<numberoftests && i< xcoord.size();++i)
        {
        	int x = xcoord.get(i);
        	int y = ycoord.get(i);
            HashSet<Integer> sepset= new HashSet<Integer>();
            MutableDouble mi = new MutableDouble(-1.0);
            if (itest.findCI(pat, adjacency, x, y, sepset, significance,mi,null))
            {
                pat.setEdge(x, y, Pattern.EdgeType.None);
                pat.setEdge(y, x, Pattern.EdgeType.None);
                sepsets.get(x).set(y,sepset);
                sepsets.get(y).set(x,sepset);
                removed.add(new Pair<Integer,Integer>(x,y));
            }
        }
	}
}
