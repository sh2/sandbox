package jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * スクリプトを読み込んで複数のワーカからコマンドを逐次発行するクラスです 。
 *
 * @author Sadao Hiratsuka
 */
public class Lock {
	/**
	 * コマンド種別です。
	 *
	 * @author Sadao Hiratsuka
	 */
	public static enum CommandType {
		SERIALIZABLE, REPEATABLE_READ, READ_COMMITTED, QUERY, UPDATE, COMMIT, ROLLBACK, SLEEP, EXIT
	}

	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPass;
	private final int sleepTime;
	private final int queryTimeout;
	private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

	private List<Command> commands = new ArrayList<>();

	/**
	 * アプリケーションの起動用メソッドです。
	 *
	 * @param args
	 *            コマンドラインオプション
	 */
	public static void main(String[] args) {
		System.out.println("Lock Inspector 1.0");

		if (args.length != 2) {
			System.out.println("usage: Java jdbc.Lock <property> <script>");
			System.exit(1);
		}

		try {
			Lock lock = new Lock(args[0]);
			lock.readScript(args[1]);
			lock.test();
		} catch (ApplicationException e) {
			e.printStackTrace();
		}
	}

	/**
	 * プロパティファイルを指定してインスタンスを構築します。
	 *
	 * @param filename
	 *            プロパティのファイル名
	 * @throws ApplicationException
	 *             プロパティファイルの読み取りに失敗した場合
	 */
	public Lock(String filename) throws ApplicationException {
		Properties property = new Properties();

		try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
			property.load(reader);
			this.jdbcUrl = property.getProperty("jdbc_url");
			this.jdbcUser = property.getProperty("jdbc_user");
			this.jdbcPass = property.getProperty("jdbc_pass");
			this.sleepTime = Integer.parseInt(property.getProperty("sleep_time"));
			this.queryTimeout = Integer.parseInt(property.getProperty("query_timeout"));
		} catch (IOException | NumberFormatException e) {
			throw new ApplicationException(e);
		}
	}

	/**
	 * スクリプトファイルを読み込みます。
	 *
	 * @param filename
	 *            スクリプトのファイル名
	 * @throws ApplicationException
	 *             スクリプトファイルの読み取りに失敗した場合
	 */
	public void readScript(String filename) throws ApplicationException {
		try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
			String commandLine = null;

			while ((commandLine = reader.readLine()) != null) {
				if (commandLine.matches("^\\s*$") || commandLine.charAt(0) == '#') {
					continue;
				}

				commands.add(new Command(commandLine));
			}
		} catch (IOException e) {
			throw new ApplicationException(e);
		}
	}

	/**
	 * スクリプトを実行します。
	 *
	 * @throws ApplicationException
	 *             スクリプトの実行に失敗した場合
	 */
	public void test() throws ApplicationException {
		Map<Integer, Worker> workers = new HashMap<>();
		Logger logger = new Logger();
		logger.start();

		try {
			for (Command command : commands) {
				if (command.getCommandType() == CommandType.SLEEP) {
					messageQueue.offer(command.toString());
					Thread.sleep(1000L * command.getSleepTime());
					messageQueue.offer("(" + command.getCommandType() + ")");
				} else {
					int workerId = command.getWorkerId();

					if (!workers.containsKey(workerId)) {
						Worker worker = new Worker(workerId);
						worker.start();
						workers.put(workerId, worker);
					}

					Worker worker = workers.get(workerId);
					worker.putCommand(command);
				}

				Thread.sleep(1000L * sleepTime);
			}
		} catch (InterruptedException e) {
			throw new ApplicationException(e);
		} finally {
			for (Worker worker : workers.values()) {
				try {
					worker.putCommand(new Command(worker.getWorkerId() + ":E"));
				} catch (ApplicationException e) {
					e.printStackTrace();
				}
			}

			for (Worker worker : workers.values()) {
				worker.join();
			}

			logger.stop();
		}
	}

	/**
	 * アプリケーション独自の例外クラスです。
	 *
	 * @author Sadao Hiratsuka
	 */
	private class ApplicationException extends Exception {
		private static final long serialVersionUID = 1L;

		/**
		 * メッセージを指定して例外を構築します。
		 *
		 * @param message
		 *            メッセージ
		 */
		public ApplicationException(String message) {
			super(message);
		}

		/**
		 * 原因を指定して例外を構築します。
		 *
		 * @param cause
		 *            原因
		 */
		public ApplicationException(Throwable cause) {
			super(cause);
		}

		/**
		 * メッセージと原因を指定して例外を構築します。
		 *
		 * @param message
		 *            メッセージ
		 * @param cause
		 *            原因
		 */
		public ApplicationException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	/**
	 * ワーカが実行するコマンドです。
	 *
	 * @author Sadao Hiratsuka
	 */
	private class Command {
		private int workerId;
		private CommandType commandType;
		private final String query;
		private final int sleepTime;

		/**
		 * コマンド文字列を指定してコマンドを構築します。
		 *
		 * @param commandLine
		 *            コマンド文字列
		 * @throws ApplicationException
		 *             コマンド文字列に誤りがある場合
		 */
		public Command(String commandLine) throws ApplicationException {
			String[] columns = commandLine.split(":");

			try {
				if (columns[0].equals("S")) {
					this.commandType = CommandType.SLEEP;
				} else {
					this.workerId = Integer.parseInt(columns[0]);

					switch (columns[1]) {
					case "SR":
						this.commandType = CommandType.SERIALIZABLE;
						break;
					case "RR":
						this.commandType = CommandType.REPEATABLE_READ;
						break;
					case "RC":
						this.commandType = CommandType.READ_COMMITTED;
						break;
					case "Q":
						this.commandType = CommandType.QUERY;
						break;
					case "U":
						this.commandType = CommandType.UPDATE;
						break;
					case "C":
						this.commandType = CommandType.COMMIT;
						break;
					case "R":
						this.commandType = CommandType.ROLLBACK;
						break;
					case "E":
						this.commandType = CommandType.EXIT;
						break;
					default:
						throw new ApplicationException("Invalid CommandType: " + commandLine);
					}
				}

				if (commandType == CommandType.QUERY || commandType == CommandType.UPDATE) {
					this.query = columns[2];
					this.sleepTime = 0;
				} else if (commandType == CommandType.SLEEP) {
					this.query = "";
					this.sleepTime = Integer.parseInt(columns[1]);
				} else {
					this.query = "";
					this.sleepTime = 0;
				}
			} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
				throw new ApplicationException("Invalid CommandLine: " + commandLine, e);
			}
		}

		/**
		 * コマンドを実行するワーカIDを返します。
		 *
		 * @return ワーカID
		 */
		public int getWorkerId() {
			return workerId;
		}

		/**
		 * コマンド種別を返します。
		 *
		 * @return コマンド種別
		 */
		public CommandType getCommandType() {
			return commandType;
		}

		/**
		 * クエリ文字列を返します。
		 *
		 * @return クエリ文字列
		 */
		public String getQuery() {
			return query;
		}

		/**
		 * スリープ時間(秒)を返します。
		 *
		 * @return スリープ時間(秒)
		 */
		public int getSleepTime() {
			return sleepTime;
		}

		@Override
		public String toString() {
			if (commandType == CommandType.QUERY || commandType == CommandType.UPDATE) {
				return workerId + ":" + commandType + ":" + query;
			} else if (commandType == CommandType.SLEEP) {
				return commandType + ":" + sleepTime;
			} else {
				return workerId + ":" + commandType;
			}
		}
	}

	/**
	 * コマンドを実行するワーカです。
	 *
	 * @author Sadao Hiratsuka
	 */
	private class Worker implements Runnable {
		private final int workerId;
		private final BlockingQueue<Command> commandQueue = new LinkedBlockingQueue<Command>();

		private volatile Thread thread;

		/**
		 * ワーカIDを指定してワーカを構築します。
		 *
		 * @param workerId
		 *            ワーカID
		 */
		public Worker(int workerId) {
			this.workerId = workerId;
		}

		/**
		 * ワーカスレッドを起動します。
		 */
		public void start() {
			if (thread == null) {
				this.thread = new Thread(this, "worker" + Integer.toString(workerId));
				thread.start();
			}
		}

		/**
		 * ワーカスレッドの終了を待ちます。
		 */
		public void join() {
			if (thread != null) {
				do {
					try {
						thread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} while (thread.isAlive());
			}
		}

		/**
		 * ワーカにコマンドを送信します。
		 *
		 * @param command
		 *            コマンド
		 */
		public void putCommand(Command command) {
			try {
				commandQueue.put(command);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * ワーカIDを返します。
		 *
		 * @return ワーカID
		 */
		public int getWorkerId() {
			return workerId;
		}

		/**
		 * コマンドを実行します。
		 */
		@Override
		public void run() {
			try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {
				connection.setAutoCommit(false);

				WORKER: while (true) {
					Command command = commandQueue.take();
					messageQueue.offer(command.toString());

					switch (command.getCommandType()) {
					case SERIALIZABLE:
						connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
						break;
					case REPEATABLE_READ:
						connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
						break;
					case READ_COMMITTED:
						connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
						break;
					case QUERY:
						try {
							executeQuery(connection, command.getQuery());
						} finally {
							messageQueue.offer("(" + command.getWorkerId() + ":"
									+ command.getCommandType() + ")");
						}

						break;
					case UPDATE:
						int count = 0;

						try {
							count = executeUpdate(connection, command.getQuery());
						} finally {
							messageQueue.offer("(" + command.getWorkerId() + ":"
									+ command.getCommandType() + ":COUNT=" + count + ")");
						}

						break;
					case COMMIT:
						connection.commit();
						break;
					case ROLLBACK:
						connection.rollback();
						break;
					case EXIT:
						break WORKER;
					}
				}
			} catch (SQLException e) {
				messageQueue.offer("(" + workerId + ":" + e + ")");
				messageQueue.offer(workerId + ":ABORT");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private void executeQuery(Connection connection, String query) throws SQLException {
			try (Statement statement = connection.createStatement()) {
				statement.setQueryTimeout(queryTimeout);

				try (ResultSet resultSet = statement.executeQuery(query)) {
					StringBuilder buffer = new StringBuilder();
					int nColumns = resultSet.getMetaData().getColumnCount();
					buffer.append("(");

					for (int columnIndex = 1; columnIndex <= nColumns; columnIndex++) {
						buffer.append(String.format("%-10s ", resultSet.getMetaData()
								.getColumnName(columnIndex)));
					}

					buffer.append(")");
					messageQueue.offer(buffer.toString());

					while (resultSet.next()) {
						buffer.setLength(0);
						buffer.append("(");

						for (int columnIndex = 1; columnIndex <= nColumns; columnIndex++) {
							buffer.append(String.format("%-10s ", resultSet.getObject(columnIndex)));
						}

						buffer.append(")");
						messageQueue.offer(buffer.toString());
					}
				}
			}
		}

		private int executeUpdate(Connection connection, String query) throws SQLException {
			try (Statement statement = connection.createStatement()) {
				statement.setQueryTimeout(queryTimeout);
				return statement.executeUpdate(query);
			}
		}
	}

	/**
	 * ログを出力するクラスです。
	 *
	 * @author Sadao Hiratsuka
	 */
	private class Logger implements Runnable {
		private Thread thread;

		/**
		 * ログを出力するためのスレッドを起動します。
		 */
		public void start() {
			if (thread == null) {
				thread = new Thread(this);
				thread.start();
			}
		}

		/**
		 * ログを出力するためのスレッドを停止します。
		 */
		public void stop() {
			if (thread != null) {
				thread.interrupt();
			}

			while (true) {
				String message = messageQueue.poll();

				if (message == null) {
					break;
				} else {
					System.out.println(message);
				}
			}
		}

		/**
		 * ログを出力します。
		 */
		@Override
		public void run() {
			while (true) {
				try {
					String message = messageQueue.take();
					System.out.println(message);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}
}
