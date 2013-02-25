package jdbc;

/**
 * MySQL Connector/J 5.1.21で追加されたAbandoned
 * connection cleanup threadの挙動を調べるクラスです。
 *
 * @author Sadao Hiratsuka
 */
public class Abandoned {
	public static void main(String[] args) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		try {
			Thread.sleep(60000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
