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

import java.util.Arrays;


public class BitBuffer {
	public BitBuffer(int bitLen, byte[] content) {
		if (content.length * 8 < bitLen) {
			throw new IllegalArgumentException("Content array too small");
		}
		this.bitLen = bitLen;
		out = content;
	}
	
	public BitBuffer() {
		out = new byte[64];
		bitLen = 0;
	}
	
	public BitBuffer(byte[] buf, int bitLen) {
		if (bitLen > buf.length * ENTRY_SIZE) {
			throw new IllegalArgumentException("Bit length too large");
		}
		out = Arrays.copyOf(buf, buf.length);
		this.bitLen = bitLen;
	}
	
	
	public void addBits(int buffer, int bitCount) {
		if (bitCount > Integer.SIZE) {
			throw new IllegalArgumentException("Bit count too large");
		}

		if (bitCount + bitLen > ENTRY_SIZE * out.length) {
			out = Arrays.copyOf(out, out.length * 2);
		}
		
		int mask = 1;
		for (int i = 0; i < bitCount; i ++) {
			if ((buffer & mask) != 0) {
				int outBitPos = bitLen & (ENTRY_SIZE - 1);
				out[bitLen / ENTRY_SIZE] |= (1 << outBitPos);
			}
			bitLen ++;
			mask <<= 1;
		}
	}
	
	public int getBit(int pos) {
		if (pos > bitLen) {
			throw new IllegalArgumentException("Bit pos larger than bit length");
		}
		
		return (out[pos / ENTRY_SIZE] & (1 << (pos & (ENTRY_SIZE - 1)))) == 0 ? 0 : 1;
	}
	
	public int getBits(int pos, int bitCount) {
		if (bitCount > Integer.SIZE) {
			throw new IllegalArgumentException("Bit count too large");
		}
		
		if (pos + bitCount > bitLen) {
			throw new IllegalArgumentException("Position and/or count are too large");
		}
		
		int ret = 0;
		for (int i = 0; i < bitCount; i ++) {
			if (getBit(i + pos) != 0) {
				ret |= (1 << i);
			}
		}
		return ret;
	}
	
	public int getBitLength() {
		return bitLen;
	}
	
	public byte[] getOutput() {
		return Arrays.copyOf(out, (7 + bitLen) / 8);
	}
	
	private static final int ENTRY_SIZE = Byte.SIZE;
	private byte out[];
	private int bitLen = 0;
}
