package smile.wide.counter;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class TestUtils {
	@Test
	public void testBitBuffer() {
		BitBuffer buf = new BitBuffer();
		Random rng = new Random(997);
		for (int i = 0; i < 1000; i ++) {
			buf.addBits(1 + rng.nextInt(5), rng.nextInt(32));
		}
		
		BitBuffer buf2 = new BitBuffer(buf.getBitLength(), buf.getOutput());
		Assert.assertEquals(buf.getBitLength(), buf2.getBitLength());
		
		for (int i = 0; i < buf.getBitLength(); i ++) {
			Assert.assertEquals(buf.getBit(i), buf2.getBit(i));
		}
	}
	
	@Test
	public void testOdometer() {
		Odometer odo = new Odometer(new int[] { 5, 3, 7 });
		int count = 0;
		do {
			count ++;
			for (int i: odo.getValue()) {
				System.out.print(i + " ");
			}
			System.out.println();
		} while (!odo.next());
			
		Assert.assertEquals(5 * 3 * 7, count);
	}
	
}
