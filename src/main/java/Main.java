import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args) {
    int port = 6379;
    boolean listening = true;

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      // Since the tester restarts your program quite often, setting
      // SO_REUSEADDR ensures that we don't run into 'Address already in use'
      // errors
      serverSocket.setReuseAddress(true);

      while (listening) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("New client connected");

        new Thread(() -> handleClient(clientSocket))
            .start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
      System.exit(-1);
    }
  }

  static void handleClient(Socket clientSocket) {
    try (clientSocket;
        OutputStream outputStream = clientSocket.getOutputStream();
        BufferedReader in = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()))) {

      while (true) {
        byte[] input = new byte[1024];
        clientSocket.getInputStream().read(input);
        String inputString = new String(input).trim();
        System.out.println("Received: " + inputString);
        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}