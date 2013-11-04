/**
 * 
 */
package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import smile.wide.algorithms.independence.ContIndependenceTest;
import smile.wide.algorithms.independence.DiscIndependenceTest;
import smile.wide.algorithms.independence.IndependenceTest;
import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public class BasicIndependenceStep extends IndependenceStep {

	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int maxAdjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) {
        IndependenceTest itest = null;
        final int nvar = ds.getNumberOfVariables();
        
        if(disc) {
        	itest = new DiscIndependenceTest(ds);
        }
        else {
        	itest = new ContIndependenceTest(ds);
        }

        // find conditional independencies
        int card = 0;
        while (true)
        {
            for (int x = 0; x < nvar; x++)
            {
                for (int y = x + 1; y < nvar; y++)
                {
                    if (pat.getEdge(x, y) == Pattern.EdgeType.None && pat.getEdge(y, x) == Pattern.EdgeType.None)
                    {
                        continue;
                    }
                    HashSet<Integer> sepset= new HashSet<Integer>();
                    if (itest.findCI(pat, card, x, y, sepset, significance))
                    {
                        pat.setEdge(x, y, Pattern.EdgeType.None);
                        pat.setEdge(y, x, Pattern.EdgeType.None);
                        sepsets.get(x).set(y,sepset);
                        sepsets.get(y).set(x,sepset);
                    }
                }
            }
            card++;
            if (card > nvar - 2 || card > maxAdjacency)
            {
                break;
            }
        }

	}

}
