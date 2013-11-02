package smile.wide.algorithms;

import java.util.ArrayList;

import smile.wide.data.DataSet;
import smile.wide.utils.Cormat;
import smile.wide.utils.SMILEMath;

public class ContIndependenceTest extends IndependenceTest {
    Cormat cm = null;

	ContIndependenceTest(DataSet tds) {
		super(tds);
        cm = new Cormat(ds);
	}
	@Override
	public double calcPValue(int x, int y, ArrayList<Integer> z) {
        // first calculate the partial correlation coefficient
        int z_size = z.size();
        double rho = cm.GetRho(x, y, z);
        // apply fisher's z-transform
        double zscore = Math.abs(0.5 * Math.sqrt((double) cm.numRecords() - z_size - 3) * Math.log((1 + rho) / (1 - rho)));
        return 2 - 2 * SMILEMath.normalcdf(zscore);
	}
	
}
