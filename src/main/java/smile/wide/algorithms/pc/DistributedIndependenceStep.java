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
	int numberoftests = 500;
	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int maxAdjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) throws Exception {
		SMILEData d = (SMILEData) ds;
		Configuration conf = new Configuration();
		conf.setBoolean("disc", disc);
		conf.setInt("maxAdjacency", maxAdjacency);
		conf.setDouble("significance", significance);
		conf.setInt("numberoftests",numberoftests);
		conf.set("datastorage","/user/mdejongh/datatmp");
		conf.set("testoutput","/user/mdejongh/testoutput");

		String[] args = {};
		DistributedIndependenceJob job = new DistributedIndependenceJob();
		job.data = d;
		job.pat = pat;
		job.sepsets = sepsets;
		ToolRunner.run(conf, job, args);
	}
}
