package smile.wide.utils;

/** represents a pair 
 * 
 * @author tomas.singliar@boeing.com
 * (stolen from http://stackoverflow.com/questions/156275/what-is-the-equivalent-of-the-c-pairl-r-in-java)
 *
 * @param <T1>
 * @param <T2>
 */

public class Pair<A, B> {
	
    private A first;
    private B second;

    public Pair() {
    	super();
    }
    
    public Pair(A first, B second) {
    	super();
    	this.first = first;
    	this.second = second;
    }

	public int hashCode() {
    	int hashFirst = first != null ? first.hashCode() : 0;
    	int hashSecond = second != null ? second.hashCode() : 0;

    	return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    public boolean equals(Object other) {
    	if (other instanceof Pair) {
    		Pair<A, B> otherPair = (Pair<A, B>) other;
    		return 
    		((  this.first == otherPair.first ||
    			( this.first != null && otherPair.first != null &&
    			  this.first.equals(otherPair.first))) &&
    		 (	this.second == otherPair.second ||
    			( this.second != null && otherPair.second != null &&
    			  this.second.equals(otherPair.second))) );
    	}

    	return false;
    }

    public String toString()
    { 
           return "(" + first + ", " + second + ")"; 
    }

    public A getFirst() {
    	return first;
    }

    public void setFirst(A a) {
    	first = a;
    }
    public B getSecond() {
    	return second;
    }
    
    public void setSecond(B b) {
    	second = b;
    }
}
