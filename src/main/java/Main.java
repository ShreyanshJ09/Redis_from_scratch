import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {

    private static final KeyValueStore keyValueStore = new KeyValueStore();

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
                String line = reader.readLine();
                if (line == null) break;

                if (!line.startsWith("*")) {
                    sendError(out, "Invalid RESP");
                    continue;
                }

                int argCount = Integer.parseInt(line.substring(1));

                String command = null;
                String key = null;
                String value = null;

                for (int i = 0; i < argCount; i++) {
                    reader.readLine(); // $length
                    String arg = reader.readLine();

                    if (i == 0) {
                        command = arg.toUpperCase();
                    } else if (i == 1) {
                        key = arg;
                    } else if (i == 2) {
                        value = arg;
                    }
                }

                switch (command) {
                    case "PING" -> sendSimpleString(out, "PONG");
                    case "ECHO" -> {
                        if (key != null) sendBulkString(out, key);
                        else sendError(out, "ECHO requires an argument");
                    }
                    case "SET" -> {
                        if (key != null && value != null) {
                            keyValueStore.set(key, value);
                            sendSimpleString(out, "OK");
                        } else {
                            sendError(out, "SET requires key and value");
                        }
                    }
                    case "GET" -> {
                        if (key != null) {
                            if (keyValueStore.exists(key)) {
                                sendBulkString(out, keyValueStore.get(key));
                            } else {
                                sendNullBulkString(out);
                            }
                        } else {
                            sendError(out, "GET requires a key");
                        }
                    }
                    default -> sendError(out, "unknown command");
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

    private static void sendNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static void sendError(OutputStream out, String msg) throws IOException {
        out.write(("-ERR " + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
