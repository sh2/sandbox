package lang;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ランダム文字列生成のスピードを測定するクラスです。
 *
 * @author Sadao Hiratsuka
 */
public class RandomString {
	private static final char[] ALPHA_NUMERIC = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A',
			'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
			'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9' };

	private final static int PACKS = (int) (Math.log(Integer.MAX_VALUE) / Math
			.log(ALPHA_NUMERIC.length));
	private final static int RANGE = (int) Math.pow(ALPHA_NUMERIC.length, PACKS);

	private final Random random = new Random();
	private final AtomicInteger count = new AtomicInteger();

	/**
	 * 起動用メソッドです。
	 *
	 * @param args
	 *            コマンドラインオプション
	 */
	public static void main(String[] args) {
		new RandomString().benchmark();
	}

	/**
	 * 長さを指定してランダム文字列を生成します。
	 *
	 * @param length
	 *            文字列長
	 * @return ランダム文字列
	 */
	public String getRandomString1(int length) {
		StringBuilder buffer = new StringBuilder();

		for (int index = 0; index < length; index++) {
			buffer.append(ALPHA_NUMERIC[random.nextInt(ALPHA_NUMERIC.length)]);
		}

		return buffer.toString();
	}

	/**
	 * 長さを指定してランダム文字列を生成します。
	 * Randomクラスのメソッド呼び出し回数を削減し、
	 * StringBuilderではなくcharの配列を使うことで高速化を図っています。
	 *
	 * @param length
	 *            文字列長
	 * @return ランダム文字列
	 */
	public String getRandomString2(int length) {
		char[] buffer = new char[length];
		int rest = 0;
		int rand = 0;

		for (int index = 0; index < length; index++) {
			if (rest == 0) {
				rand = random.nextInt(RANGE);
				rest = PACKS;
			}

			buffer[index] = ALPHA_NUMERIC[rand % ALPHA_NUMERIC.length];
			rand /= ALPHA_NUMERIC.length;
			rest--;
		}

		return new String(buffer);
	}

	private void benchmark() {
		ScheduledExecutorService progress = Executors.newSingleThreadScheduledExecutor();
		progress.scheduleAtFixedRate(new Progress(), 0L, 1L, TimeUnit.SECONDS);

		for (int i = 0; i < 1000000; i++) {
			getRandomString1(100);
			count.incrementAndGet();
		}

		try {
			Thread.sleep(3000L);
		} catch (InterruptedException e) {
			//
		}

		System.out.println("packs : " + PACKS);
		System.out.println("range : " + RANGE);

		for (int i = 0; i < 4000000; i++) {
			getRandomString2(100);
			count.incrementAndGet();
		}

		progress.shutdownNow();
	}

	private class Progress implements Runnable {
		private int previousCount = 0;

		public void run() {
			int currentCount = RandomString.this.count.intValue();
			System.out.println(currentCount - previousCount);
			previousCount = currentCount;
		}
	}
}
