package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import smile.wide.data.DataSet;
import smile.wide.data.SMILEData;
import smile.wide.utils.Pattern;

/** Basic Implementation of the PC algorithm
 * Takes a DataSet as input and returns a Pattern
 * @author m.a.dejongh@gmail.com
 */
public class PC {
	public IndependenceStep istep = new BasicIndependenceStep();
	/** maxAdjacency limits the size of the conditioning set
	 *  that is available for the independence tests*/
    public int maxAdjacency = 8;
    /** signficance determines the significance level to be
     *  used by the independence tests*/
    public double significance=0.05;
    
	/** sepsetHas function, checks if a variable e is present
	 * in the separator set of variables x and y 
	 * @param container of all sepsets, variables x, y, and e
	 * @return true or false (depending if e is present in the sepset)
	 */	
    private boolean sepsetHas(ArrayList<ArrayList<Set<Integer> > > sepsets, int x, int y, int e)
    {
        // check if the given sepset contains element e
        Set<Integer> sepset = sepsets.get(x).get(y);
        if(sepset == null)
        	return false;
        return sepset.contains(e);
    }

	/** Learn function, takes dataset as input.
	 *  learns a network structure using the PC algorithm
	 *  returns a PDAG, or Pattern.
	 *  @param dataset
	 *  @return pattern.
	 */	
    public Pattern Learn( DataSet ds) 
    {
    	Pattern pat = new Pattern();
        int nvar = ds.getNumberOfVariables();
        int n = ds.getNumberOfRecords();
        if (n < 3)
        {
        	throw new IllegalArgumentException("pc: too few data points");
        }
        if (nvar < 2)
        {
        	throw new IllegalArgumentException("pc: too few variables");
        }

        // check if all vars are discrete or continuous
        boolean disc = false;
        boolean cont = false;
        for (int v = 0; v < nvar; v++)
        {
            if (ds.isDiscrete(v))
            {
                disc = true;
            }
            else
            {
                cont = true;
            }
        }

        if (disc && cont)
        {
        	throw new IllegalArgumentException("pc: a mix of continuous and discrete variables is not allowed");
        }

        // check for constants
        ArrayList<Double> tmp = new ArrayList<Double>(Collections.nCopies(nvar, 0.0));
        ArrayList<Boolean> prob = new ArrayList<Boolean>(Collections.nCopies(nvar, false));
        int nprob = 0;
        int r;
        for (r = 0; nprob < nvar && r < n; r++)
        {
            for (int c = 0; nprob < nvar && c < nvar; c++)
            {
                if (ds.isDiscrete(c))
                {
                    if (r == 0)
                    {
                        tmp.set(c,(double) ds.getInt(c, r));//DOUBLE CHECK IF THIS IS OK
                    }
                    else
                    {
                        if (!prob.get(c) && tmp.get(c) != ds.getInt(c, r))
                        {
                            prob.set(c,true);
                            nprob++;
                        }
                    }
                }
                else
                {
                    if (r == 0)
                    {
                        tmp.set(c,ds.getDouble(c, r));
                    }
                    else
                    {
                        if (!prob.get(c) && tmp.get(c) != ds.getDouble(c, r))
                        {
                            prob.set(c,true);
                            nprob++;
                        }
                    }
                }
            }
        }
        if (nprob != nvar)
        {
            String vars = "";
            for (int i = 0; i < (int) prob.size(); i++)
            {
                if (!prob.get(i))
                {
                    vars += " " + ds.getId(i);
                }
            }
            //throw exception with message?
        	throw new IllegalArgumentException("pc: constant variables not allowed: "+vars);
        }
        // check if there are no missing values
        for (r = 0; r < n; r++)
        {
            for (int v = 0; v < nvar; v++)
            {
                if (ds.isMissing(v, r))
                {
                	//missing values
                	throw new IllegalArgumentException("pc: a missing values not allowed");
                }
            }
        }

        // step 1: create fully connected undirected graph
        // initialize pattern
        pat.setSize(nvar);
        int i;
        for (i = 0; i < nvar; i++)
        {
            for (int j = 0; j < nvar; j++)
            {
                if (i == j)
                {
                    pat.setEdge(i, j, Pattern.EdgeType.None);
                }
                else
                {
                    pat.setEdge(i, j, Pattern.EdgeType.Undirected);
                }
            }
        }

        // step 2: check for conditional independencies
        // create sepsets
        ArrayList<ArrayList<Set<Integer> > > sepsets = new ArrayList<ArrayList<Set<Integer> > >();
        for (int x=0; x< nvar; x++) {
        	sepsets.add(new ArrayList<Set<Integer> >());
        }
        for (i = 0; i < nvar; i++)
        {
            for (int x=0; x< nvar; x++) {
            	sepsets.get(i).add(new HashSet<Integer>());
            }
        }
        //Execute the independence test step
        try {
			istep.execute(ds, pat, disc, maxAdjacency, significance, sepsets);
		} catch (Exception e) {
			e.printStackTrace();
		}

        // step 3: orient edges as v-structure
        for (i = 0; i < nvar; i++)
        {
            for (int adj1 = 0; adj1 < nvar; adj1++)
            {
                if (i != adj1 && (pat.getEdge(adj1, i) != Pattern.EdgeType.None || pat.getEdge(i, adj1) != Pattern.EdgeType.None))
                {
                    for (int adj2 = adj1 + 1; adj2 < nvar; adj2++)
                    {
                        if (i != adj2 && (pat.getEdge(adj2, i) != Pattern.EdgeType.None || pat.getEdge(i, adj2) != Pattern.EdgeType.None))
                        {
                            if (pat.getEdge(adj1, adj2) == Pattern.EdgeType.None && pat.getEdge(adj2, adj1) == Pattern.EdgeType.None && !sepsetHas(sepsets, adj1, adj2, i))
                            {
                                pat.setEdge(adj1, i, Pattern.EdgeType.Directed);
                                pat.setEdge(adj2, i, Pattern.EdgeType.Directed);
                                if (pat.getEdge(i, adj1) == Pattern.EdgeType.Undirected)
                                {
                                    pat.setEdge(i, adj1, Pattern.EdgeType.None);
                                }
                                if (pat.getEdge(i, adj2) == Pattern.EdgeType.Undirected)
                                {
                                    pat.setEdge(i, adj2, Pattern.EdgeType.None);
                                }
                            }
                        }
                    }
                }
            }
        }

        // step 4: orient remaining edges
        boolean update = true;
        while (update)
        {
            update = false;
            // a) orient x -> y - z as x -> y -> z
            for (i = 0; i < nvar; i++)
            {
                for (int adj1 = 0; adj1 < nvar; adj1++)
                {
                    for (int adj2 = adj1 + 1; adj2 < nvar; adj2++)
                    {
                        if (i != adj1 && i != adj2 && pat.getEdge(adj1, i) == Pattern.EdgeType.Directed && pat.getEdge(adj2, i) == Pattern.EdgeType.Undirected && pat.getEdge(adj1, adj2) == Pattern.EdgeType.None && pat.getEdge(adj2, adj1) == Pattern.EdgeType.None)
                        {
                            if (!pat.hasDirectedPath(adj2, i))
                            {
                                pat.setEdge(i, adj2, Pattern.EdgeType.Directed);
                                pat.setEdge(adj2, i, Pattern.EdgeType.None);
                                update = true;
                            }
                        }
                        else
                        {
                            if (i != adj1 && i != adj2 && pat.getEdge(adj2, i) == Pattern.EdgeType.Directed && pat.getEdge(adj1, i) == Pattern.EdgeType.Undirected && pat.getEdge(adj1, adj2) == Pattern.EdgeType.None && pat.getEdge(adj2, adj1) == Pattern.EdgeType.None)
                            {
                                if (!pat.hasDirectedPath(adj1, i))
                                {
                                    pat.setEdge(i, adj1, Pattern.EdgeType.Directed);
                                    pat.setEdge(adj1, i, Pattern.EdgeType.None);
                                    update = true;
                                }
                            }
                        }
                    }
                }
            }
            // b) orient x - z as x -> z if there is a path x -> ... -> z
            for (int x = 0; x < nvar; x++)
            {
                for (int y = x + 1; y < nvar; y++)
                {
                    if (pat.getEdge(x, y) == Pattern.EdgeType.Undirected)
                    {
                        // search for directed path from x -> y and y -> x
                        boolean xy = pat.hasDirectedPath(x, y);
                        boolean yx = pat.hasDirectedPath(y, x);
                        if (xy && yx)
                        {
                            assert(false);
                        }
                        else
                        {
                            if (xy)
                            {
                                pat.setEdge(x, y, Pattern.EdgeType.Directed);
                                pat.setEdge(y, x, Pattern.EdgeType.None);
                                update = true;
                            }
                            else
                            {
                                if (yx)
                                {
                                    pat.setEdge(y, x, Pattern.EdgeType.Directed);
                                    pat.setEdge(x, y, Pattern.EdgeType.None);
                                    update = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        // done
        return pat;
    }
	public static void main(String args[])
	{
		SMILEData ds = new SMILEData();
		ds.Read("../input/Hepar14k.txt");
		Pattern pat = new Pattern();
		PC alg = new PC();
		//alg.istep = new HadoopIndependenceStep();
		alg.istep = new DistributedIndependenceStep();
		alg.maxAdjacency = 8;
		alg.significance = 0.05;
		pat = alg.Learn(ds);
		pat.Print();
	}
}

