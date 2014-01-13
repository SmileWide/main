/**
 * 
 */
package smile.wide.algorithms.pc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableDouble;
import smile.wide.algorithms.independence.ContIndependenceTest;
import smile.wide.algorithms.independence.DiscIndependenceTest;
import smile.wide.algorithms.independence.DiscMITest;
import smile.wide.algorithms.independence.IndependenceTest;
import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;
import smile.wide.utils.PlainDataCounter;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public class BasicIndependenceStep extends IndependenceStep {

	class MIComparator implements Comparator<Integer> {
		public ArrayList<Double> mi_values;

		MIComparator(ArrayList<Double> m) {
			mi_values = m;
		}
		/**compare function*/
		@Override
		public int compare(Integer arg0, Integer arg1) {
			return mi_values.get(arg0.intValue()).compareTo(mi_values.get(arg1.intValue()));
		}
	}
	
	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int maxAdjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) {
        IndependenceTest itest = null;
        final int nvar = ds.getNumberOfVariables();
        
        if(disc) {
        	itest = new DiscIndependenceTest(ds,null);
        	//itest = new DiscMITest(ds,null);
        }
        else {
        	itest = new ContIndependenceTest(ds);
        }

        //1d arrays for nodes
        ArrayList<Integer> nodes = new ArrayList<Integer>();
        for(int x=0;x<nvar;++x)
        	nodes.add(x);
        ArrayList<Double> node_array = new ArrayList<Double>();
        //2d array for MI calculations
        ArrayList<ArrayList<Double>> mi_array = new ArrayList<ArrayList<Double>>();
        for(int x = 0; x< nvar; x++) {
        	node_array.add(0.0);
        	mi_array.add(new ArrayList<Double>());
        	for(int y = 0; y < nvar; y++) {
        		mi_array.get(x).add(0.0);
        	}
        }
        // find conditional independencies
        int card = 0;
        while (true)
        {
//        	System.out.println(nodes);
        	int counter=0;
            for (int x = 0; x < nvar; x++)
            {
            	int xx = nodes.get(x);
            	int xcnt=0;
            	for(int i=0;i<nvar;++i) {
            		if(i!= xx && pat.getEdge(xx, i) != Pattern.EdgeType.None)
            			xcnt++;
            	}
            	for (int y = x + 1; y < nvar; y++)
                {
                	int yy = nodes.get(y);
                	
                	if (pat.getEdge(xx, yy) == Pattern.EdgeType.None && pat.getEdge(yy, xx) == Pattern.EdgeType.None) {
                        continue;
                    }

                	int ycnt=0;
                	for(int i=0;i<nvar;++i) {
                		if(i!=yy && pat.getEdge(yy, i) != Pattern.EdgeType.None)
                			ycnt++;
                	}
                	
                    HashSet<Integer> sepset= new HashSet<Integer>();
                    MutableDouble mi = new MutableDouble(0.0);
                    if (itest.findCI(pat, card, xx, yy, sepset, significance, mi))
                    {
                    	if(card > 0) {
                    		//decrease node MI
                    		node_array.set(xx, node_array.get(xx) - mi_array.get(xx).get(yy));
                    		node_array.set(yy, node_array.get(yy) - mi_array.get(yy).get(xx));
                    	}
                    	//set MI to 0 after edge is independent
                    	mi_array.get(xx).set(yy,0.0);
                    	mi_array.get(yy).set(xx,0.0);
                    	//remove edge
                        pat.setEdge(xx, yy, Pattern.EdgeType.None);
                        pat.setEdge(yy, xx, Pattern.EdgeType.None);
                        //store sepset
                        sepsets.get(xx).set(yy,sepset);
                        sepsets.get(yy).set(xx,sepset);
                        counter++;
                    }
                    else {
                    	if(card == 0) {
                    		mi_array.get(xx).set(yy, mi.doubleValue());
                    		mi_array.get(yy).set(xx, mi.doubleValue());
                    		node_array.set(xx, node_array.get(xx) + mi.doubleValue());
                    		node_array.set(yy, node_array.get(yy) + mi.doubleValue());
                    	}
                    }
                }
            }
			System.out.println("Removed " + counter + " edges this iteration!");
			//sort node_array and nodes
			Collections.sort(nodes, new MIComparator(node_array));
			//save MI matrix to csv file
			BufferedWriter w;
			try {
				w = new BufferedWriter(new FileWriter("MImatrix_nvar"+nvar+"_"+card+".csv"));
				for(int i=0;i<nvar;++i) {
					if(i==0) {
						for(int z=0;z<nvar;++z) {
							w.write(";v"+z);
						}
						w.write("\n");
					}
					w.write("v"+i);
					for(int j=0;j<nvar;++j) {
						w.write(";"+mi_array.get(i).get(j));
					}
					w.write("\n");
				}
				w.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            card++;
            if (card > nvar - 2 || card > maxAdjacency)
            {
                break;
            }
        }

	}

}
