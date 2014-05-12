package smile.wide.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.algorithms.chen.ChenJob;
import smile.wide.algorithms.fang.FangJob;
import smile.wide.algorithms.pc.DistributedIndependenceStep;
import smile.wide.algorithms.pc.HadoopIndependenceStep;
import smile.wide.algorithms.pc.PC;
import smile.wide.data.SMILEData;

public class ExperimentCode {
	MutableInt jc = new MutableInt(0);
	public ExperimentCode(MutableInt jobcount) {
		jc = jobcount;
	}

	//runexp code
	//write pattern to file
	public void Runexp(Pattern pat, AlgWrapper alg, int mappers, String[] args) throws Exception {
		alg.runAlg(pat,mappers, args, jc);
	}
	
	public void WritePat(String name, Pattern pat) {
		BufferedWriter v;
		int nvar = pat.getSize();
		try {
			v = new BufferedWriter(new FileWriter(name+"_result_network_"+nvar+".vna"));
			v.write("*node data\nID\n");
			for(int z=0;z<nvar;++z) {
				v.write("v"+z+"\n");
			}
			v.write("*tie data\nfrom to strength\n");
			for(int i=0;i<nvar;++i) {
				for(int j=i+1;j<nvar;++j) {
					if(pat.getEdge(i, j)==Pattern.EdgeType.Undirected && pat.getEdge(j, i)==Pattern.EdgeType.Undirected)
						v.write("v"+i+" v"+j+" 1.0\n");
					if(pat.getEdge(i, j)==Pattern.EdgeType.Directed)
						v.write("v"+i+" v"+j+" 2.0\n");
					if(pat.getEdge(j, i)==Pattern.EdgeType.Directed)
						v.write("v"+j+" v"+i+" 2.0\n");
				}
			}
			v.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public abstract class AlgWrapper {
		SMILEData ds = null;
		public AlgWrapper(SMILEData dds) {
			ds = dds;
		}
		abstract public void runAlg(Pattern pat, int mappers, String[] args, MutableInt jc) throws Exception;
		public void splitData(int nrsplits) throws IOException, InterruptedException {
			//We save the data to a temporary file.
			ds.Write("presplit.txt");
			try {
				//remove header from file
				Process p = Runtime.getRuntime().exec("sed -i 1d presplit.txt");
				p.waitFor();
				//using the command line we split the data in smaller files using limit
				p = Runtime.getRuntime().exec("split -a 5 -d -l "+ (ds.getNumberOfRecords() / nrsplits) + " presplit.txt split-");
				p.waitFor();
				//remove original file
				p = Runtime.getRuntime().exec("rm presplit.txt");
				p.waitFor();
				//remove current input files on the cluster
				p = Runtime.getRuntime().exec(new String[] { "sh", "-c","/usr/bin/hadoop fs -rm /user/mdejongh/input/*"});
				p.waitFor();
				//upload new files to cluster
				p = Runtime.getRuntime().exec(new String[] {"sh", "-c","/usr/bin/hadoop fs -copyFromLocal split-* /user/mdejongh/input"});
				p.waitFor();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//remove splitted files
			File file=new File(".");
			File[] files = file.listFiles();
			for(File f : files) {
				if(f.getName().contains("split-")) {
					f.delete();
				}
			}

		}
	}
	
	public class WrapperChen extends AlgWrapper {
		public WrapperChen(SMILEData dds) {
			super(dds);
		}
		@Override
		public void runAlg(Pattern pat, int mappers, String[] args, MutableInt jc) throws Exception {
			splitData(mappers);
			Configuration conf = new Configuration();
			conf.setInt("nvar", ds.getNumberOfVariables());
			conf.setFloat("epsilon", (float) 0.01);
			conf.set("datainput", "/user/mdejongh/input");
			conf.set("countoutput", "/user/mdejongh/counts");
			conf.set("countlist","mycounts.txt");
			int exitCode = ToolRunner.run(conf, new ChenJob(pat,jc), args);//args need to be filled
		}
	}
	
	public class WrapperFang extends AlgWrapper {
		public WrapperFang(SMILEData dds) {
			super(dds);
		}
		@Override
		public void runAlg(Pattern pat, int mappers, String[] args, MutableInt jc) throws Exception {
			splitData(mappers);
			//define node ordering here.
			int nvar = ds.getNumberOfVariables();
			Configuration conf = new Configuration();
			conf.setInt("nvar", nvar);
			conf.setInt("maxsetsize", 8);
			conf.set("datainput", "/user/mdejongh/input");
			conf.set("countoutput", "/user/mdejongh/counts");
			conf.set("structureoutput","/user/mdejongh/beststructure");
			conf.set("countlist","mycounts.txt");
			conf.set("beststructure","beststructure.txt");
			
			ArrayList<Integer> order = new ArrayList<Integer>();
			for(int x=0;x<nvar;++x)
				order.add(x);

//			long seed = 1982;
//			Collections.shuffle(order, new Random(seed));
			
			FangJob f = new FangJob(pat,jc);
			f.setOrder(order);
			int exitCode = ToolRunner.run(conf, f, args);
		}
	}
	
	public class WrapperPCA extends AlgWrapper {
		public WrapperPCA(SMILEData dds) {
			super(dds);
		}
		@Override
		public void runAlg(Pattern pat, int mappers, String[] args, MutableInt jc) throws Exception {
			splitData(mappers);
			PC alg = new PC();
			alg.istep = new HadoopIndependenceStep(jc);
			alg.maxAdjacency = 8;
			alg.significance = 0.05;
			alg.Learn(ds,pat);
		}
	}

	public class WrapperPCB extends AlgWrapper {
		public WrapperPCB(SMILEData dds) {
			super(dds);
		}
		@Override
		public void runAlg(Pattern pat, int mappers, String[] args, MutableInt jc) throws Exception {
			PC alg = new PC();
			alg.istep = new DistributedIndependenceStep(jc);
			alg.maxAdjacency = 8;
			alg.significance = 0.05;
			alg.Learn(ds,pat);
		}
	}
	
	public static void main(String args[]) throws Exception {
		SMILEData ds = new SMILEData();
		String inputfile = args[2];
		String algorithm = args[3];
		String extracted;
		extracted = inputfile.substring(1+inputfile.lastIndexOf("/"),inputfile.lastIndexOf("."));
		
		ds.Read(inputfile);
		
		MutableInt jobcount = new MutableInt(0);
		Pattern pat = new Pattern();
		ExperimentCode exp = new ExperimentCode(jobcount);
		AlgWrapper alg = null;
		int a = Integer.decode(algorithm);
		String name="";
		switch(a) {
			case 1:
				name = "chen";
				alg = exp.new WrapperChen(ds);
				break;
			case 2:
				name= "fang";
				alg = exp.new WrapperFang(ds);
				break;
			case 3:
				name= "pca";
				alg = exp.new WrapperPCA(ds);
				break;
			case 4:
				name= "pcb";
				alg = exp.new WrapperPCB(ds);
				break;
		}
		name+="_"+extracted;
		long time = System.currentTimeMillis();
		exp.Runexp(pat, alg, 220, args);
		long completedIn = System.currentTimeMillis() - time;
		System.out.println("Time (ms): "+completedIn);
		System.out.println("Time: "+DurationFormatUtils.formatDuration(completedIn, "HH:mm:ss:SS"));
		System.out.println("MR Jobs: "+jobcount.intValue());
		exp.WritePat(name,pat);
		
		//write results to file
		PrintWriter out = new PrintWriter(name+"_results.txt");
		out.println("Time (ms): "+completedIn);
		out.println("Time: "+DurationFormatUtils.formatDuration(completedIn, "HH:mm:ss:SS"));
		out.println("MR Jobs: "+jobcount.intValue());
		out.close();
		
		//On PC:
		//load result pattern
		//load original network
		//generate pattern
		//compare patterns
		//- Hamming
		//- Skeleton
		//- others
	}
}
