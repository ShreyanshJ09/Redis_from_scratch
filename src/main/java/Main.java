import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Main {

    private static final KeyValueStore keyValueStore = new KeyValueStore();
    private static final ListStore listStore = new ListStore();

    public static void main(String[] args) throws IOException {
        int port = 6379;
        startExpiryThread();
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
                String[] args = new String[argCount];
                for (int i = 0; i < argCount; i++) {
                    reader.readLine(); // $length
                    args[i] = reader.readLine();
                }
                command = args[0].toUpperCase();
                if (argCount >= 2) key = args[1];
                if (argCount >= 3) value = args[2];
                switch (command) {
                    case "PING" -> sendSimpleString(out, "PONG");
                    case "ECHO" -> {
                        if (key != null) sendBulkString(out, key);
                        else sendError(out, "ECHO requires an argument");
                    }
                    case "SET" -> {
                        if (key != null && value != null) {
                            long expiryMillis = Long.MAX_VALUE; // default: no expiry

                            // Check for optional arguments
                            if (argCount >= 5) {
                                String optionName = args[3].toUpperCase();

                                try {
                                    int timeValue = Integer.parseInt(args[4]); // actual time
                                    if ("EX".equals(optionName)) {
                                        expiryMillis = timeValue * 1000; // seconds to ms
                                    } else if ("PX".equals(optionName)) {
                                        expiryMillis = timeValue; // already in ms
                                    }
                                } catch (NumberFormatException ignored) {}
                            }

                            keyValueStore.set(key, value, expiryMillis);
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
                    case "RPUSH" -> {
                        int size = 0;
                        for (int i = 2; i < argCount; i++) {
                            WakeUpResult wake = listStore.rpush(key, args[i]);
                            if (wake != null) {
                                // Wake blocked BLPOP client
                                sendArray(
                                    wake.client.out,
                                    List.of(wake.client.key, wake.value)
                                );
                            }
                        }
                        size = listStore.llen(key);
                        sendInteger(out, size);
                    }
                    case "LPUSH" -> {
                        int size = 0;
                        for (int i = argCount-1; i >= 2; i--) {
                            WakeUpResult wake = listStore.rpush(key, args[i]);
                            if (wake != null) {
                                // Wake blocked BLPOP client
                                sendArray(
                                    wake.client.out,
                                    List.of(wake.client.key, wake.value)
                                );
                            }
                            size = listStore.llen(key);
                        }
                        sendInteger(out, size);
                    }
                    case "LRANGE" -> {
                        if (argCount == 4) {
                            try {
                                int start = Integer.parseInt(args[2]);
                                int stop = Integer.parseInt(args[3]);

                                List<String> elements = listStore.lrange(key, start, stop);
                                sendArray(out, elements);
                            } catch (NumberFormatException e) {
                                sendError(out, "LRANGE start and stop must be integers");
                            }
                        } else {
                            sendError(out, "LRANGE requires key start stop");
                        }
                    }
                    case "LLEN" -> {
                        sendInteger(out, listStore.llen(key));
                    }
                    case "LPOP" -> {
                        if (key == null) {
                            sendError(out, "LPOP requires a key");
                            break;
                        }

                        // LPOP key
                        if (argCount == 2) {
                            String first_value = listStore.lpop(key);
                            if (first_value == null) {
                                sendNullBulkString(out);
                            } else {
                                sendBulkString(out, first_value);
                            }
                            break;
                        }
                        if (argCount == 3) {
                            try {
                                int count = Integer.parseInt(args[2]);
                                List<String> popped = listStore.lpop(key, count);
                                sendArray(out, popped);
                            } catch (NumberFormatException e) {
                                sendError(out, "count must be an integer");
                            }
                            break;
                        }

                        sendError(out, "wrong number of arguments for LPOP");
                    }
                    case "BLPOP" -> {
                        String list_value = listStore.lpop(key);
                        System.err.println(argCount);
                        long timeoutMs = 0;
                        if (argCount >= 3) {
                            timeoutMs = (long)(Double.parseDouble(args[2]) * 1000);
                        }

                        // If value exists → return immediately
                        if (list_value != null) {
                            sendArray(out, List.of(key, list_value));
                            continue;
                        }

                        // Otherwise block client
                        if (timeoutMs == 0){
                            listStore.blockClient(key, out, Long.MAX_VALUE);
                        } else {
                            listStore.blockClient(key, out, timeoutMs);
                        }

                        // VERY IMPORTANT
                        // Do NOT send response
                        // Do NOT close socket
                        continue;
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

    private static void sendInteger(OutputStream out, int value) throws IOException {
        out.write((":" + value + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    private static void sendArray(OutputStream out, List<String> items) throws IOException {
        out.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

        for (String item : items) {
            out.write(("$" + item.length() + "\r\n" + item + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        out.flush();
    }
    private static void startExpiryThread() {
        new Thread(() -> {
            while (true) {
                List<BlockedClient> expired =
                    listStore.collectExpiredBlockedClients();

                for (BlockedClient client : expired) {
                    try {
                        sendNullBulkString(client.out);
                    } catch (IOException ignored) {}
                }

                try { Thread.sleep(10); } catch (Exception ignored) {}
            }
        }).start();
    }

}
