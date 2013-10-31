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
package smile.wide.data;

// TODO: name the package something useful

/**
 * Represents instances read from a file.
 * 
 * Each instance must have an identifier because this is intended
 * for environments where we have no control over the order 
 * in which instances are processed.
 * 
 * Then, the instance has a vector of atribute values.
 * TODO: this should be smartened up to include sparse representation, etc. 
 *       But hopefully the data munging question will be addressed more 
 *       comprehensively in XDATA.
 * 
 * @author tomas.singliar@boeing.com
 *
 * @param The data type of 
 * @param <Value>
 */
public class Instance<ID,Value> {
	
	    private ID _id;
	    private Value[] _val;
	    
	    public Instance(ID id, Value[] v){
	        this._id = id;
	        this._val = v;
	    }
	    
	    public ID getID()
	    { 
	    	return _id; 
	    }
	    	    
	    public Value[] getValue()
	    { 
	    	return _val; 
	    }
	    
	    public void setID(ID id)
	    { 
	    	this._id = id; 
	    }
	    
	    public void setValue(Value[] v)
	    { 
	    	this._val = v; 
	    }

}
