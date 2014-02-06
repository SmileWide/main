package smile.wide.utils;

import java.util.ArrayList;
import java.util.Collections;

/** Pattern class, contains PDAGs aka
 * Patterns. Is used to represent the
 * structure learned from data using
 * the PC algorithm
 * @author m.a.dejongh@gmail.com
 */
public class Pattern {
	/**definition for possible types of edges*/
	public enum EdgeType {None, Undirected, Directed};
	/**2d array that contains the adjacency matrix*/
	ArrayList<ArrayList<EdgeType> > mat = new ArrayList<ArrayList<EdgeType> >();
	/**Empty default constructor*/
	public Pattern() {	
	}
	
	/**constructor, creates pattern from string*/
	public Pattern(String value) {
		String[] data = value.split("#");
		int size = Integer.decode(data[0]);
		setSize(size);
		char z;
		for(int y=0;y<size;++y) {
			for(int x=0;x<size;++x) {
				z = data[1].charAt(x+y*size);
	            switch(z) {
	            case '0': setEdge(y,x,EdgeType.None);break;
	            case '1': setEdge(y,x,EdgeType.Undirected);break;
	            case '2': setEdge(y,x,EdgeType.Directed);break;
	            }
			}
		}
	}
	
	/**returns number of variables
	 * @return number of variables
	 * */
	public int getSize()
	{
	    return mat.size();
	}

	/**sets size of pattern
	 * sets number of variables
	 * @param size number of variables
	 */
	public void setSize(int size)
	{
		mat = new ArrayList<ArrayList<EdgeType> >();
	    for (int i = 0; i < size; i++)
	    {
	    	mat.add(new ArrayList<EdgeType>(Collections.nCopies(size,EdgeType.None)));
	    }
	}

	/**returns edge between from and to
	 * @param from node
	 * @param to node
	 * @return edge
	 */
	public EdgeType getEdge(int from, int to)
	{
	    return mat.get(from).get(to);
	}

	/**sets edge between from and to
	 * @param from node
	 * @param to node
	 * @param type edge
	 */
	public void setEdge(int from, int to, EdgeType type)
	{
	    mat.get(from).set(to,type);
	}

	/**function checks if there is
	 * a directed path between from and to
	 * @param from node
	 * @param to node
	 * @return true/false
	 */
	public boolean hasDirectedPath(int from, int to) {
	    ArrayList<Boolean> visited = new ArrayList<Boolean>(Collections.nCopies(getSize(), false));
	    return hasDirectedPathRec(this, from, to, visited);
	}

	/**helper function for checking if there
	 * is a directed path between from and to
	 * @param pat current pattern
	 * @param from node
	 * @param to node
	 * @param visited set indicating which nodes have been visited
	 * @return true/false
	 */
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

	/**function checks if graph has a cycle
	 * @return true/false
	 */
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

	/**checks if graph is a Directed Acyclic Graph
	 * @return true/false
	 */
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

	/**Prints pattern to screen*/
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
	
	@Override 
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int size = getSize();
		sb.append(size);
		sb.append("#");
	    for (int i = 0; i < size; i++) {
	        for (int j = 0; j < size; j++) {
	            switch(getEdge(i,j)) {
	            case None:sb.append("0");break;
	            case Undirected:sb.append("1");break;
	            case Directed:sb.append("2");break;
	            }
	        }
	    }
		return sb.toString();
	}
}
