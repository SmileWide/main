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
package smile.wide.counter;

public class Odometer {

	public Odometer(int[] range) {
		this.range = range;
		this.value = new int[range.length];
	}
	
	public boolean next() {
		int i;
		for (i = 0; i < range.length; i ++) {
			if (++ value[i] == range[i]) {
				value[i] = 0;
			} else {
				break;
			}
		}
		
		return i == range.length;
	}
	
	public int[] getValue() {
		return value;
	}
	
	public int getTotalSpinCount() {
		int count = 1;
		for (int r: range) {
			count *= r;
		}
		return count;
	}
	
	private int[] range;
	private int[] value;

}
