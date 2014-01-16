package smile.wide.algorithms.independence;

import java.util.HashSet;
import java.util.Set;

import smile.wide.utils.Pattern;
import smile.wide.utils.Pattern.EdgeType;

public class MinimumCutSet {
	Pattern pat = null;
	
	public MinimumCutSet(Pattern p) {
		pat = p;
	}
	
	boolean depthFirstSearchPathsV2(Set<Integer> marked, Set<Integer> result, int current, int target, int caller) {
		boolean onpath = false;
		if( !marked.contains(current) ){
			marked.add(current);
			if(pat.getEdge(current, target) != Pattern.EdgeType.None)
			{
				result.add(current);
				return true;
			}
			for(int i = 0; i < pat.getSize();++i) {
				if(i!= caller && pat.getEdge(current, i) != Pattern.EdgeType.None) {
					if(depthFirstSearchPathsV2(marked,result,i,target,current)) {
						onpath = true;
						result.add(current);
					}
				}
			}
		} else {
			if(result.contains(current))
				return true;
		}
		return onpath;
	}

	boolean depthFirstSearchPaths(Set<Integer> marked, Set<Integer> result, int current, int target) {
		boolean onpath = false;
		if( !marked.contains(current) ){
			marked.add(current);
			if(pat.getEdge(current, target) != Pattern.EdgeType.None)
			{
				result.add(current);
				return true;
			}
			for(int i = 0; i < pat.getSize();++i) {
				if(pat.getEdge(current, i) != Pattern.EdgeType.None) {
					if(depthFirstSearchPaths(marked,result,i,target)) {
						onpath = true;
						result.add(current);
					}
				}
			}
		}
		return onpath;
	}
	
	public Set<Integer> cutSet(int x, int y) {
		boolean edge_exists = false;

		if(pat.getEdge(x, y) != EdgeType.None) {
			edge_exists = true;
			pat.setEdge(x, y, EdgeType.None);
			pat.setEdge(y, x, EdgeType.None);
		}
		
		Set<Integer> SX = new HashSet<Integer>();

		for(int i=0;i<pat.getSize();++i) {
			if(pat.getEdge(x, i) != EdgeType.None) {
				SX.add(i);
			}
		}
		
		Set<Integer> marked = new HashSet<Integer>();
		Set<Integer> adjpath = new HashSet<Integer>();
		depthFirstSearchPathsV2(marked,adjpath,x,y,x);
		adjpath.remove(x);

		SX.retainAll(adjpath);
		
		if(edge_exists) {
			pat.setEdge(x, y, EdgeType.Undirected);
			pat.setEdge(y, x, EdgeType.Undirected);
		}
		return SX;
	}

	public Set<Integer> minimumCutSet(int x, int y) {
		boolean edge_exists = false;
		if(pat.getEdge(x, y) != EdgeType.None) {
			edge_exists = true;
			pat.setEdge(x, y, EdgeType.None);
			pat.setEdge(y, x, EdgeType.None);
		}
		
		Set<Integer> Xnbr = new HashSet<Integer>();
		Set<Integer> Ynbr = new HashSet<Integer>();
		Set<Integer> SX = new HashSet<Integer>();
		Set<Integer> SY = new HashSet<Integer>();

		for(int i=0;i<pat.getSize();++i) {
			if(pat.getEdge(x, i) != EdgeType.None) {
				Xnbr.add(i);
				SX.add(i);
			}
			if(pat.getEdge(y, i) != EdgeType.None) {
				Ynbr.add(i);
				SY.add(i);
			}
		}
		
		Set<Integer> marked = new HashSet<Integer>();
		Set<Integer> adjpath = new HashSet<Integer>();
		depthFirstSearchPaths(marked,adjpath,x,y);
		adjpath.remove(x);

		SX.retainAll(adjpath);
		SY.retainAll(adjpath);

		//get neighbours of i from Sx, substract Sx from neighbours
		Set<Integer> temp = new HashSet<Integer>();
		Set<Integer> SXp = new HashSet<Integer>();
		for(Integer j : SX) {
			temp.clear();
			for(int i=0;i<pat.getSize();++i) {
				if(pat.getEdge(j, i) != EdgeType.None) {
					temp.add(i);
				}
			}			
			temp.removeAll(SX);
			SXp.addAll(temp);
		}
		SXp.retainAll(adjpath);

		//get neighbours of i from Sy, substract Sy from neighbours
		Set<Integer> SYp = new HashSet<Integer>();
		for(Integer j : SY) {
			temp.clear();
			for(int i=0;i<pat.getSize();++i) {
				if(pat.getEdge(j, i) != EdgeType.None) {
					temp.add(i);
				}
			}			
			temp.removeAll(SY);
			SYp.addAll(temp);
		}
		SYp.retainAll(adjpath);
		
		//return smallest of two subsets
		Set<Integer> result = null;
		if(SXp.size() < SYp.size())
			result = SXp;
		else
			result = SYp;
		
		if(edge_exists) {
			pat.setEdge(x, y, EdgeType.Undirected);
			pat.setEdge(y, x, EdgeType.Undirected);
		}
		return result;
	}
}
