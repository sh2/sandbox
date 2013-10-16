package net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
	public static void main(String[] args) {
		try (ServerSocket listener = new ServerSocket();) {
			listener.setReuseAddress(true);
			listener.bind(new InetSocketAddress(3306));
			while (true) {
				try (Socket socket = listener.accept();) {
					try {
						Thread.sleep(10000L);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					socket.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}