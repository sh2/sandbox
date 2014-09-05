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

public class Lock {
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

	public static void main(String[] args) {
		System.out.println("Lock Inspector");

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

	private void readScript(String filename) throws ApplicationException {
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

	private void test() throws ApplicationException {
		Map<Integer, Worker> workers = new HashMap<>();
		Logger logger = new Logger();
		logger.start();

		try {
			for (Command command : commands) {
				if (command.getCommandType() == CommandType.SLEEP) {
					messageQueue.put(command.toString());
					Thread.sleep(1000L * command.getSleepTime());
					System.out.println("(" + command.getWorkerId() + ":" + command.getCommandType()
							+ ")");
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
		} catch (SQLException | InterruptedException e) {
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
		}
	}

	private class ApplicationException extends Exception {
		private static final long serialVersionUID = 1L;

		public ApplicationException(String message) {
			super(message);
		}

		public ApplicationException(Throwable cause) {
			super(cause);
		}

		public ApplicationException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	private class Command {
		private int workerId;
		private CommandType commandType;
		private final String query;
		private final int sleepTime;

		public Command(String commandLine) throws ApplicationException {
			String[] columns = commandLine.split(":");

			try {
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
				case "S":
					this.commandType = CommandType.SLEEP;
					break;
				case "E":
					this.commandType = CommandType.EXIT;
					break;
				default:
					throw new ApplicationException("Invalid CommandType: " + commandLine);
				}

				if (commandType == CommandType.QUERY || commandType == CommandType.UPDATE) {
					this.query = columns[2];
					this.sleepTime = 0;
				} else if (commandType == CommandType.SLEEP) {
					this.query = "";
					this.sleepTime = Integer.parseInt(columns[2]);
				} else {
					this.query = "";
					this.sleepTime = 0;
				}
			} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
				throw new ApplicationException("Invalid CommandLine: " + commandLine, e);
			}
		}

		public int getWorkerId() {
			return workerId;
		}

		public CommandType getCommandType() {
			return commandType;
		}

		public String getQuery() {
			return query;
		}

		public int getSleepTime() {
			return sleepTime;
		}

		@Override
		public String toString() {
			if (commandType == CommandType.QUERY || commandType == CommandType.UPDATE) {
				return workerId + ":" + commandType + ":" + query;
			} else if (commandType == CommandType.SLEEP) {
				return workerId + ":" + commandType + ":" + sleepTime;
			} else {
				return workerId + ":" + commandType;
			}
		}
	}

	private class Worker implements Runnable {
		private final int workerId;
		private final BlockingQueue<Command> commandQueue = new LinkedBlockingQueue<Command>();

		private volatile Thread thread;

		public Worker(int workerId) throws SQLException {
			this.workerId = workerId;
		}

		public void start() {
			if (thread == null) {
				this.thread = new Thread(this, "worker" + Integer.toString(workerId));
				thread.start();
			}
		}

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

		public void putCommand(Command command) {
			try {
				commandQueue.put(command);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public int getWorkerId() {
			return workerId;
		}

		@Override
		public void run() {
			try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {
				connection.setAutoCommit(false);

				WORKER: while (true) {
					Command command = commandQueue.take();
					System.out.println(command);

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
							System.out.println("(" + command.getWorkerId() + ":"
									+ command.getCommandType() + ")");
						}

						break;
					case UPDATE:
						int count = 0;

						try {
							count = executeUpdate(connection, command.getQuery());
						} finally {
							System.out.println("(" + command.getWorkerId() + ":"
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
				System.out.println("(" + workerId + ":" + e + ")");
				System.out.println(workerId + ":ABORT");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private void executeQuery(Connection connection, String query) throws SQLException {
			try (Statement statement = connection.createStatement()) {
				statement.setQueryTimeout(queryTimeout);

				try (ResultSet resultSet = statement.executeQuery(query)) {
					int nColumns = resultSet.getMetaData().getColumnCount();
					System.out.print("(");

					for (int columnIndex = 1; columnIndex <= nColumns; columnIndex++) {
						System.out.printf("%-10s ",
								resultSet.getMetaData().getColumnName(columnIndex));
					}

					System.out.println(")");

					while (resultSet.next()) {
						System.out.print("(");

						for (int columnIndex = 1; columnIndex <= nColumns; columnIndex++) {
							System.out.printf("%-10s ", resultSet.getObject(columnIndex));
						}

						System.out.println(")");
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

	private class Logger implements Runnable {
		private Thread thread;

		public void start() {
			if (thread == null) {
				thread = new Thread(this);
				thread.start();
			}
		}

		public void stop() {
			thread.interrupt();

			while (true) {
				String message = messageQueue.poll();

				if (message == null) {
					break;
				} else {
					System.out.println(message);
				}
			}
		}

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
