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
package smile.wide.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//
/** Writable class for send SMILE network (in String form) and structure Score
 * @author m.a.dejongh@gmail.com
 */
public class StrucLogLikeWritable implements Writable {
	/** Contains SMILE XML description of network*/
	Text nw = new Text("");
	/** Writable Double that contains the network structure's score*/
	DoubleWritable loglik = new DoubleWritable(Double.NEGATIVE_INFINITY);

	public StrucLogLikeWritable() {
		nw = new Text("");
		loglik = new DoubleWritable(Double.NEGATIVE_INFINITY);;
	}

	public StrucLogLikeWritable(String net, double ll) {
		nw = new Text(net);
		loglik = new DoubleWritable(ll);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		nw.readFields(in);
		loglik.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nw.write(out);
		loglik.write(out);
	}

	public void setNW(Text net) {
		nw = net;
	}

	public void setNW(String net) {
		nw = new Text(net);
	}

	public void setLogLike(DoubleWritable ll) {
		loglik = ll;
	}

	public void setLogLike(double ll) {
		loglik = new DoubleWritable(ll);
	}
	
	public String getNw() {
		return nw.toString();
	}

	public double getLogLike() {
		return loglik.get();
	}

	@Override
	public String toString() {
		return nw + "\t" + loglik;
	}
}

