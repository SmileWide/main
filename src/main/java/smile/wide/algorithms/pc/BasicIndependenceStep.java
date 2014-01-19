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
import smile.wide.algorithms.independence.IndependenceTest;
import smile.wide.algorithms.labelpropagation.LProp;
import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;

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
        MutableDouble mi = new MutableDouble(0.0);

        //for MI clusters
        LProp lp = new LProp();
		ArrayList<Integer> clusters = new ArrayList<Integer>();

		//node exclusion
		ArrayList<ArrayList<Boolean>> excluded = new ArrayList<ArrayList<Boolean>>();
		for(int x=0;x<nvar;++x) {
			excluded.add(new ArrayList<Boolean>());
			for(int y=0;y<nvar;++y)
				excluded.get(x).add(false);
		}
			
		
        double tot =( nvar * (nvar -1)) / 2.0;
        while (true) {
            double iter_counter =0;
        	int counter=0;
            for (int x = 0; x < nvar; x++) {
            	int xx = nodes.get(x);
            	for (int y = x + 1; y < nvar; y++) {
                    iter_counter++;
                    if(iter_counter % 100 == 0)
                    	System.out.println(" "+ 100.0*(iter_counter / tot));	
                	int yy = nodes.get(y);
                	if (pat.getEdge(xx, yy) == Pattern.EdgeType.None && pat.getEdge(yy, xx) == Pattern.EdgeType.None) {
                        continue;
                    }
                    HashSet<Integer> sepset= new HashSet<Integer>();
                    mi.setValue(0.0);
                    if (itest.findCI(pat, card, xx, yy, sepset, significance, mi, excluded)) {
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

			
			//calc clusters
			lp.CalcClusters(clusters,mi_array);
			//save MI matrix to csv file (need format that allows writing of cluster ids)
			BufferedWriter w,v;
			try {
				w = new BufferedWriter(new FileWriter("mi_network_"+nvar+"_"+card+".vna"));
				v = new BufferedWriter(new FileWriter("edge_network_"+nvar+"_"+card+".vna"));
				w.write("*node data\nID cluster\n");
				v.write("*node data\nID cluster\n");
				for(int z=0;z<nvar;++z) {
					w.write("v"+z+" "+clusters.get(z)+"\n");
					v.write("v"+z+" "+clusters.get(z)+"\n");
				}
				w.write("*tie data\nfrom to strength\n");
				v.write("*tie data\nfrom to strength\n");
				for(int i=0;i<nvar;++i) {
					for(int j=0;j<nvar;++j) {
						if(mi_array.get(i).get(j) > 0.0)
							w.write("v"+i+" v"+j+" "+mi_array.get(i).get(j)+"\n");
						if(pat.getEdge(i, j)!=Pattern.EdgeType.None)
							v.write("v"+i+" v"+j+" 1.0\n");
					}
				}
				w.close();
				v.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			card++;
            if (card > nvar - 2 || card > maxAdjacency) {
                break;
            }
        }
	}
}
