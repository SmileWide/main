/**
 * 
 */
package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.data.DataSet;
import smile.wide.data.SMILEData;
import smile.wide.utils.Pattern;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public class DistributedIndependenceStep extends IndependenceStep {
	/**basic code framework to execute hadoop style stuff will be replaced with actual code*/
	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int maxAdjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) throws Exception {
		SMILEData d = (SMILEData) ds;
		Configuration conf = new Configuration();
		conf.setInt("nvar", d.getNumberOfVariables());
		conf.setBoolean("disc", disc);
		
		int adj_limit = maxAdjacency;
        if(disc) {
            int min=2;
            int nvar = ds.getNumberOfVariables();
            for(int x=0;x<nvar;++x)
            {
            	int nstates = ds.getStateNames(x).length;
                 if(x==0)
                   min = nstates;
                 else
                 {
                      if(nstates < min)
                        min = nstates;
                 }
            }
            adj_limit = (int) (Math.log(ds.getNumberOfRecords() / 5.0)/Math.log(min)-2);
        }
        if(adj_limit < maxAdjacency)
        	maxAdjacency = adj_limit;
		conf.setInt("maxAdjacency", maxAdjacency);
		conf.setFloat("significance", (float) significance);
		conf.set("datastorage","/user/mdejongh/datatmp");
		conf.set("testoutput","/user/mdejongh/testoutput");
		String[] args = {};
		PartitionIndependenceJob job = new PartitionIndependenceJob();
		job.data = d;
		job.pat = pat;
		job.sepsets = sepsets;
		ToolRunner.run(conf, job, args);
	}
}
