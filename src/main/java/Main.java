import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {

    public static void main(String[] args) throws IOException {
        int port = 6379;

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Redis-like server running on port " + port);

        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Client connected");

            new Thread(() -> handleClient(client)).start();
        }
    }

    private static void handleClient(Socket client) {
        try (
            InputStream in = client.getInputStream();
            OutputStream out = client.getOutputStream();
        ) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8)
            );

            while (true) {
                // Read first RESP line (e.g. *1 or *2)
                String line = reader.readLine();
                if (line == null) break;

                if (!line.startsWith("*")) {
                    sendError(out, "Invalid RESP");
                    continue;
                }

                int argCount = Integer.parseInt(line.substring(1));

                String command = null;
                String argument = null;

                for (int i = 0; i < argCount; i++) {
                    reader.readLine(); // $length
                    String value = reader.readLine();

                    if (i == 0) {
                        command = value.toUpperCase();
                    } else if (i == 1) {
                        argument = value;
                    }
                }

                if ("PING".equals(command)) {
                    sendSimpleString(out, "PONG");
                } else if ("ECHO".equals(command) && argument != null) {
                    sendBulkString(out, argument);
                } else {
                    sendError(out, "unknown command");
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected");
        }
    }

    private static void sendSimpleString(OutputStream out, String msg) throws IOException {
        out.write(("+" + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static void sendBulkString(OutputStream out, String msg) throws IOException {
        out.write(("$" + msg.length() + "\r\n" + msg + "\r\n")
                .getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static void sendError(OutputStream out, String msg) throws IOException {
        out.write(("-ERR " + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}