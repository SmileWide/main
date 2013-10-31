/*
             Licensed to the DARPA XDATA project.
       DARPA XDATA licenses this file to you under the 
         Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
           You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
                 either express or implied.                    
   See the License for the specific language governing
     permissions and limitations under the License.
 */
package smile.wide.utils;

/**
 * Represents a pair of arbitrary types.
 * 
 * @author tomas.singliar@boeing.com
 * @see <a href="http://stackoverflow.com/questions/156275/what-is-the-equivalent-of-the-c-pairl-r-in-java" target="blank">Stackoverflow</a>
 * 
 * @param <T1>	The type of the first item in the pair.
 * @param <T2>	The type of the second item in the pair.
 */
public class Pair<T1, T2>
{
private final T1 first;
private final T2 second;

/**
 * Instantiate a Pair.
 * @param first
 * @param second
 */
public Pair(T1 first, T2 second)
	{
	super();
	this.first = first;
	this.second = second;
	}


public int hashCode()
	{
	int hashFirst = first != null ? first.hashCode() : 0;
	int hashSecond = second != null ? second.hashCode() : 0;

	return (hashFirst + hashSecond) * hashSecond + hashFirst;
	}


public boolean equals(Object other)
	{
	if (other instanceof Pair)
		{
		Pair<?,?> otherPair = (Pair<?,?>) other;
		return
			(this.first == otherPair.first || 
			  (this.first != null && otherPair.first != null && this.first.equals(otherPair.first))) 
			&& 
			(this.second == otherPair.second || 
			  (this.second != null && otherPair.second != null && this.second.equals(otherPair.second)));
		}

	return false;
	}


public String toString()
	{
	return "(" + first + ", " + second + ")";
	}


public T1 getFirst()
	{
	return first;
	}


public T2 getSecond()
	{
	return second;
	}
}
