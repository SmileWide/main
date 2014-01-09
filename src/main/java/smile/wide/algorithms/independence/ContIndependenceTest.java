package smile.wide.algorithms.independence;

import java.util.ArrayList;

import org.apache.commons.lang.mutable.MutableDouble;

import smile.wide.data.DataSet;
import smile.wide.utils.Cormat;
import smile.wide.utils.SMILEMath;

/** Independence test for continuous data 
 * (Assumes Gaussian distributions)
 * Is initialized with a DataSet
 * calculates p-value of test
 * @author m.a.dejongh@gmail.com
 */
public class ContIndependenceTest extends IndependenceTest {
	/**Correlation matrix datastructure*/
	Cormat cm = null;

	/** Constructor
	 * calculates correlation matrix
	 * @param tds
	 */
	public ContIndependenceTest(DataSet tds) {
		super(tds);
        cm = new Cormat(ds);
	}
	/**Independence test,returns p-value of
	 * continuous test
	 * @param x variable
	 * @param y variable
	 * @param z set of conditioning variables
	 */
	@Override
	public double calcPValue(int x, int y, ArrayList<Integer> z, MutableDouble mi) {
        // first calculate the partial correlation coefficient
        int z_size = z.size();
        double rho = cm.GetRho(x, y, z);
        // apply fisher's z-transform
        double zscore = Math.abs(0.5 * Math.sqrt((double) cm.numRecords() - z_size - 3) * Math.log((1 + rho) / (1 - rho)));
        return 2 - 2 * SMILEMath.normalcdf(zscore);
	}
	
}
