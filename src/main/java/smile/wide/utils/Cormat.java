package smile.wide.utils;

import java.util.ArrayList;
import smile.wide.data.DataSet;

public class Cormat {
    int nvar;
    int n;
    ArrayList<ArrayList<Double> > cm=null;

	
	public Cormat(DataSet ds)
	{
	    nvar = ds.getNumberOfVariables();
	    n = ds.getNumberOfRecords();
	    boolean cont = true;
	    for (int v = 0; v < nvar; v++)
	    {
	        if (ds.isDiscrete(v))
	        {
	            cont = false;
	        }
	    }
	    if (cont)
	    {
	    	cm.ensureCapacity(nvar);
	        for (int i = 0; i < nvar; i++)
	        {
	        	cm.get(i).ensureCapacity(nvar);
	        }
	        for (int x = 0; x < nvar; x++)
	        {
	            cm.get(x).set(x,1.0);
	            for (int y = x + 1; y < nvar; y++)
	            {
	                double sumx2 = 0;
	                double sumy2 = 0;
	                double sumxy = 0;
	                double meanx = ds.getDouble(x, 0);
	                double meany = ds.getDouble(y, 0);
	                for (int i = 2; i <= n; i++)
	                {
	                    double sweep = ((double) i - 1) / i;
	                    double deltax = ds.getDouble(x, i - 1) - meanx;
	                    double deltay = ds.getDouble(y, i - 1) - meany;
	                    sumx2 += deltax * deltax * sweep;
	                    sumy2 += deltay * deltay * sweep;
	                    sumxy += deltax * deltay * sweep;
	                    meanx += deltax / i;
	                    meany += deltay / i;
	                }
	                double popsdx = Math.sqrt(sumx2 / n);
	                double popsdy = Math.sqrt(sumy2 / n);
	                double covxy = sumxy / n;
	                double rho = covxy / (popsdx * popsdy);
	                cm.get(x).set(y,rho);
	                cm.get(y).set(x,rho);
	            }
	        }
	    }
	}

	public int numVars()
	{
	    return nvar;
	}

	public int numRecords()
	{
	    return n;
	}

	public double GetRho(int x, int y, ArrayList<Integer> z)
	{
	    // TODO: caching
	    if (z.size() == 0)
	    {
	        return cm.get(x).get(y);
	    }
	    else
	    {
	    	ArrayList<Integer> newz = new ArrayList<Integer>();
	    	for(Integer i : z) {
	    	    newz.add(new Integer(i));
	    	}
	    	int z0 = newz.get(newz.size()-1);
	    	newz.remove(newz.size()-1);
	        double rho_xy = GetRho(x, y, newz);
	        double rho_xz0 = GetRho(x, z0, newz);
	        double rho_yz0 = GetRho(y, z0, newz);
	        return (rho_xy - rho_xz0 * rho_yz0) / (Math.sqrt(1 - rho_xz0 * rho_xz0) * Math.sqrt(1 - rho_yz0 * rho_yz0));
	    }
	}

}
