package smile.wide;

import smile.wide.algorithms.pc.HadoopIndependenceStep;
import smile.wide.algorithms.pc.PC;
import smile.wide.data.SMILEData;
import smile.wide.utils.Pattern;

public class PCTest {
	public static void main(String args[])
	{
		SMILEData ds = new SMILEData();
		System.out.println("Loading test data");
		ds.Read("input/Hepar14k.txt");
		Pattern pat = new Pattern();
		PC alg = new PC();
		//alg.istep = new HadoopIndependenceStep();
		alg.maxAdjacency = 8;
		alg.significance = 0.05;
		//Test the PC class with same dataset. print out result
		System.out.println("Test PC class");
		pat = alg.Learn(ds);
		System.out.println();
		System.out.println("Result");
		pat.Print();
	}
}
