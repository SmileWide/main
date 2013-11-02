package smile.wide.utils;

import java.util.ArrayList;
import java.util.Collections;

public class Pattern {
	
	public enum EdgeType {None, Undirected, Directed};
	ArrayList<ArrayList<EdgeType> > mat = new ArrayList<ArrayList<EdgeType> >();
	
	public int getSize()
	{
	    return mat.size();
	}

	public void setSize(int size)
	{
		mat = new ArrayList<ArrayList<EdgeType> >();
	    for (int i = 0; i < size; i++)
	    {
	    	mat.add(new ArrayList<EdgeType>(Collections.nCopies(size,EdgeType.None)));
	    }
	}

	public EdgeType getEdge(int from, int to)
	{
	    return mat.get(from).get(to);
	}

	public void setEdge(int from, int to, EdgeType type)
	{
	    mat.get(from).set(to,type);
	}

	boolean hasDirectedPathRec(Pattern pat, int from, int to, ArrayList<Boolean> visited)
	{
	    // don't go through bi-directed edges!
	    int nvar = pat.getSize();
	    if (from == to) {
	        return true;
	    }
	    else {
	        visited.set(from,true);
	        for (int i = 0; i < nvar; i++) {
	            if (!visited.get(i) && pat.getEdge(from, i) == EdgeType.Directed && pat.getEdge(i, from) != EdgeType.Directed) {
	                if (hasDirectedPathRec(pat, i, to, visited) ) {
	                    return true;
	                }
	            }
	        }
	    }
	    return false;
	}

	public boolean hasDirectedPath(int from, int to) {
	    ArrayList<Boolean> visited = new ArrayList<Boolean>(Collections.nCopies(getSize(), false));
	    return hasDirectedPathRec(this, from, to, visited);
	}

	public boolean hasCycle() {
	    int size = mat.size();
	    for (int i = 0; i < size; i++) {
	        for (int j = 0; j < size; j++) {
	            if (i != j && hasDirectedPath(i, j) && hasDirectedPath(j, i) ) {
	                return true;
	            }
	        }
	    }
	    return false;
	}

	public boolean IsDAG() {
	    // check for undirected and bidirectional edges
	    int nvar = mat.size();
	    for (int i = 0; i < nvar; i++) {
	        for (int j = i + 1; j < nvar; j++) {
	            if (getEdge(i, j) == EdgeType.Undirected || getEdge(j, i) == EdgeType.Undirected || (getEdge(i, j) == EdgeType.Directed && getEdge(j, i) == EdgeType.Directed)) {
	                return false;
	            }
	        }
	    }
	    // check for cycles
	    return !hasCycle();
	}

	boolean hasIncomingEdge(int to) {
	    for (int from = 0; from < getSize(); from++) {
	        if (from == to) continue;
	        if (getEdge(from, to) == EdgeType.Directed) {
	            return true;
	        }
	    }
	    return false;
	}

	boolean hasOutgoingEdge(int from) {
	    for (int to = 0; to < getSize(); to++) {
	        if (from == to) continue;
	        if (getEdge(from, to) == EdgeType.Directed) {
	            return true;
	        }
	    }
	    return false;
	}

	public void Print() {
	    int size = mat.size();
	    System.out.println("None: 0");
	    System.out.println("Undirected: 1");
	    System.out.println("Directed: 2");
	    for (int i = 0; i < size; i++) {
	        for (int j = 0; j < size; j++) {
	            if (j != 0) {
	            	System.out.print(" ");
	            }
	            switch(getEdge(i,j)) {
	            case None: System.out.print("0");break;
	            case Undirected: System.out.print("1");break;
	            case Directed: System.out.print("2");break;
	            }
	            
	        }
	        System.out.println();
	    }
	}

	void GetAdjacentNodes(final int node, ArrayList<Integer> adj) {
		adj.clear();
		final int size = mat.size();
		for(int i=0;i < size; ++i) {
			if(i!=node) {
				if(mat.get(node).get(i) != EdgeType.None) {
					adj.add(i);
				}
				else if(mat.get(i).get(node) != EdgeType.None) {
					adj.add(i);
				}
			}
		}
	}
}
