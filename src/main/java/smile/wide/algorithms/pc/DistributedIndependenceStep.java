/**
 * 
 */
package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.data.DataSet;
import smile.wide.data.SMILEData;
import smile.wide.hadoop.io.RandSeedInputFormat;
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
		conf.setInt("maxAdjacency", maxAdjacency);
		conf.setFloat("significance", (float) significance);
		conf.set("datastorage","/user/mdejongh/datatmp");
		conf.set("testoutput","/user/mdejongh/testoutput");
		conf.setInt(RandSeedInputFormat.CONFKEY_SEED_COUNT, 2000);//number of mappers to be run
		conf.setInt(RandSeedInputFormat.CONFKEY_WARMUP_ITER, 100000);
		String[] args = {};
		DistributedIndependenceJob job = new DistributedIndependenceJob();
		job.data = d;
		job.pat = pat;
		job.sepsets = sepsets;
		ToolRunner.run(conf, job, args);
	}
}
