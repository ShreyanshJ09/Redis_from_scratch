import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.lang.ThreadLocal;


public class Main {

    private static final KeyValueStore keyValueStore = new KeyValueStore();
    private static final ListStore listStore = new ListStore();
    private static final StreamStore streamStore = new StreamStore();
    private static final ThreadLocal<TransactionState> transactionState =
    ThreadLocal.withInitial(TransactionState::new);
    
    private static String serverRole = "master";
    private static String masterHost = null;
    private static int masterPort = 0;
    private static int replicaListeningPort = 6379;
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final int MASTER_REPL_OFFSET = 0;
    public static void main(String[] args) throws IOException {
        int port = 6379;
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                try {
                    port = Integer.parseInt(args[i + 1]);
                    replicaListeningPort = port;
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port: " + args[i + 1]);
                }
            } 
            else if (args[i].equals("--replicaof") && i + 1 < args.length) {
                String replicaofValue = args[i + 1];
                String[] parts = replicaofValue.split(" ");
                
                if (parts.length == 2) {
                    masterHost = parts[0];
                    try {
                        masterPort = Integer.parseInt(parts[1]);
                        serverRole = "slave";
                        System.out.println("Replica of " + masterHost + ":" + masterPort);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid master port: " + parts[1]);
                    }
                } else {
                    System.err.println("Invalid --replicaof format");
                }
            }
        }
        
        System.out.println("Server role: " + serverRole);
        
        startExpiryThread();
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Redis-like server running on port " + port);
        
        // If this is a replica, initiate handshake with master
        if (serverRole.equals("slave")) {
            initiateReplicaHandshake();
        }
        
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
            
            TransactionState txState = transactionState.get();

            while (true) {
                String line = reader.readLine();
                if (line == null) break;

                if (!line.startsWith("*")) {
                    sendError(out, "Invalid RESP");
                    continue;
                }

                int argCount = Integer.parseInt(line.substring(1));
                String[] args = new String[argCount];
                for (int i = 0; i < argCount; i++) {
                    reader.readLine(); // $length
                    args[i] = reader.readLine();
                }
                String command = args[0].toUpperCase();
                if (command.equals("MULTI")) {
                    txState.startTransaction();
                    sendSimpleString(out, "OK");
                    continue;
                }

                if (command.equals("EXEC")) {
                    if (!txState.isInTransaction()) {
                        sendError(out, "EXEC without MULTI");
                        continue;
                    }

                    executeTransaction(out, txState);
                    txState.endTransaction();
                    System.out.println("EXEC: transaction completed");
                    continue;
                }
                if (txState.isInTransaction()) {
                    txState.queueCommand(command, args);
                    sendSimpleString(out, "QUEUED");
                    System.out.println("Queued: " + command);
                    continue;
                }

                executeCommand(command, args, out);
            }
        }   catch (IOException e) {
        System.out.println("Client disconnected");
    } finally {
        // CRITICAL: Clean up thread-local to prevent memory leaks
        transactionState.remove();
        System.out.println("Transaction state cleaned up");
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
    
    private static void sendStreamEntries(OutputStream out, List<StreamEntry> entries)throws IOException  {
        out.write(("*" + entries.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (StreamEntry entry : entries) {
            out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
            
            String id = entry.getId();
            out.write(("$" + id.length() + "\r\n" + id + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            Map<String, String> fields = entry.getFields();
            int fieldCount = fields.size() * 2;
            out.write(("*" + fieldCount + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            for (Map.Entry<String, String> field : fields.entrySet()) {
                String fieldName = field.getKey();
                String fieldValue = field.getValue();
                out.write(("$" + fieldName.length() + "\r\n" + fieldName + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.write(("$" + fieldValue.length() + "\r\n" + fieldValue + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
        out.flush();
    }
    
    private static void sendXReadMultipleResponse(OutputStream out, List<StreamResult> results)throws IOException {
        // Send array of streams
        out.write(("*" + results.size() + "\r\n").getBytes());
        
        for (StreamResult result : results) {
            // Each stream: [key, [entries]]
            out.write("*2\r\n".getBytes());
            
            // Stream key
            out.write(("$" + result.key.length() + "\r\n" + result.key + "\r\n").getBytes());
            
            // Entries (same format as before)
            out.write(("*" + result.entries.size() + "\r\n").getBytes());

            sendStreamEntries(out, result.entries);
        }
        out.flush();
    }

    private static void executeTransaction(OutputStream out, TransactionState txState) throws IOException {
        List<QueuedCommand> commands = txState.getCommandQueue();
        
        if (commands.isEmpty()) {
            out.write("*0\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            System.out.println("EXEC: empty transaction");
            return;
        }
        
        List<ByteArrayOutputStream> responses = new ArrayList<>();
        
        for (QueuedCommand cmd : commands) {
            ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
            
            try {
                executeCommand(cmd.command, cmd.args, responseBuffer);
                responses.add(responseBuffer);
            } catch (Exception e) {
                ByteArrayOutputStream errorBuffer = new ByteArrayOutputStream();
                String errorMsg = e.getMessage() != null ? e.getMessage() : "command failed";
                errorBuffer.write(("-ERR " + errorMsg + "\r\n").getBytes(StandardCharsets.UTF_8));
                responses.add(errorBuffer);
            }
        }
        
        out.write(("*" + responses.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (ByteArrayOutputStream response : responses) {
            out.write(response.toByteArray());
        }
        out.flush();
        
        System.out.println("EXEC: executed " + commands.size() + " commands");
    }
    private static void executeCommand(String command, String[] args, OutputStream out) throws IOException {
        String key = args.length >= 2 ? args[1] : null;
        String value = args.length >= 3 ? args[2] : null;
        int argCount = args.length;
        
        switch (command) {
                    case "PING" -> sendSimpleString(out, "PONG");
                    case "INFO" -> {
                        String section = args.length >= 2 ? args[1].toLowerCase() : "all";
                        
                        if (section.equals("replication") || section.equals("all")) {
                            StringBuilder info = new StringBuilder();
                            info.append("# Replication\r\n");
                            info.append("role:").append(serverRole).append("\r\n");
                            info.append("master_replid:").append(MASTER_REPLID).append("\r\n");
                            info.append("master_repl_offset:").append(MASTER_REPL_OFFSET).append("\r\n");
                            sendBulkString(out, info.toString());
                        } else {
                            sendBulkString(out, "");
                        }
                    }
                    case "ECHO" -> {
                        if (key != null) sendBulkString(out, key);
                        else sendError(out, "ECHO requires an argument");
                    }
                    case "SET" -> {
                        if (key != null && value != null) {
                            long expiryMillis = Long.MAX_VALUE;
                            if (argCount >= 5) {
                                String optionName = args[3].toUpperCase();
                                try {
                                    int timeValue = Integer.parseInt(args[4]);
                                    if ("EX".equals(optionName)) {
                                        expiryMillis = timeValue * 1000;
                                    } else if ("PX".equals(optionName)) {
                                        expiryMillis = timeValue;
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
                    case "INCR" -> {
                        if (key == null) {
                            sendError(out, "INCR requires a key");
                            return;
                        }
                        
                        try {
                            long newValue = keyValueStore.increment(key);
                            sendInteger(out, (int) newValue);
                            System.out.println("INCR: " + key + " -> " + newValue);
                        } catch (IllegalArgumentException e) {
                            sendError(out, e.getMessage());
                            System.out.println("INCR error: " + key + " - " + e.getMessage());
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
                        for (int i = 2; i < argCount; i++) {
                            WakeUpResult wake = listStore.lpush(key, args[i]);
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
                            return;
                        }
                        if (argCount == 2) {
                            String first_value = listStore.lpop(key);
                            if (first_value == null) {
                                sendNullBulkString(out);
                            } else {
                                sendBulkString(out, first_value);
                            }
                            return;
                        }
                        if (argCount == 3) {
                            try {
                                int count = Integer.parseInt(args[2]);
                                List<String> popped = listStore.lpop(key, count);
                                sendArray(out, popped);
                            } catch (NumberFormatException e) {
                                sendError(out, "count must be an integer");
                            }
                            return;
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

                        if (list_value != null) {
                            sendArray(out, List.of(key, list_value));
                            return;
                        }
                        if (timeoutMs == 0){
                            listStore.blockClient(key, out, Long.MAX_VALUE);
                        } else {
                            listStore.blockClient(key, out, timeoutMs);
                        }

                        return;
                    }
                    case "XADD" -> {
                        if (argCount < 4 || argCount % 2 == 0) {
                            System.err.println(argCount);
                            sendError(out, "wrong number of arguments for XADD");
                            return;
                        }

                        String streamKey = args[1];
                        String entryId = args[2];
                        
                        Map<String, String> fields = new LinkedHashMap<>();
                        for (int i = 3; i < argCount; i += 2) {
                            String fieldName = args[i];
                            String fieldValue = args[i + 1];
                            fields.put(fieldName, fieldValue);
                        }
                        try {
                            String addedId = streamStore.xadd(streamKey, entryId, fields);
                            sendBulkString(out, addedId);
                        } catch (IllegalArgumentException e) {
                            System.out.println("XADD validation error: " + e.getMessage());
                            sendError(out, e.getMessage());
                        }
                    }
                    case "TYPE" -> {
                        if (key == null) {
                            sendError(out, "TYPE requires a key");
                            return;
                        }
                        if (streamStore.exists(key)) {
                            sendSimpleString(out, "stream");
                        }
                        else if (keyValueStore.exists(key)) {
                            sendSimpleString(out, "string");
                        } else {
                            sendSimpleString(out, "none");
                        }
                    }
                    case "XRANGE" -> {
                        if (argCount < 4) {
                            sendError(out, "wrong number of arguments for XRANGE");
                            return;
                        }
                        String streamKey = args[1];
                        String startId = args[2];
                        String endId = args[3];
                        
                        List<StreamEntry> entries = streamStore.xrange(streamKey, startId, endId);
                        sendStreamEntries(out, entries);
                    }
                    case "XREAD" -> {
                        long blockMs = 0;
                        int argsStartIdx = 2;
                        
                        if (argCount >= 4 && args[1].equalsIgnoreCase("BLOCK")) {
                            blockMs = Long.parseLong(args[2]);
                            argsStartIdx = 4;
                        }
                        
                        if (!args[argsStartIdx - 1].equalsIgnoreCase("STREAMS")) {
                            sendError(out, "XREAD requires STREAMS keyword");
                            return;
                        }
                        
                        int argsAfterStreams = argCount - argsStartIdx;
                        if (argsAfterStreams % 2 != 0) {
                            sendError(out, "Unbalanced XREAD STREAMS list");
                            return;
                        }
                        
                        int numStreams = argsAfterStreams / 2;
                        String[] keys = new String[numStreams];
                        String[] ids = new String[numStreams];
                        
                        for (int i = 0; i < numStreams; i++) {
                            keys[i] = args[argsStartIdx + i];
                            ids[i] = args[argsStartIdx + numStreams + i];
                        }
                        
                        List<StreamResult> results = new ArrayList<>();
                        for (int i = 0; i < numStreams; i++) {
                            List<StreamEntry> entries = streamStore.xread(keys[i], ids[i]);
                            results.add(new StreamResult(keys[i], entries));
                        }
                        
                        boolean hasData = false;
                        for (StreamResult r : results) {
                            if (!r.entries.isEmpty()) {
                                hasData = true;
                                break;
                            }
                        }
                        
                        if (hasData) {
                            sendXReadMultipleResponse(out, results);
                        } else if (blockMs > 0) {
                            long deadline = System.currentTimeMillis() + blockMs;
                            
                            while (System.currentTimeMillis() < deadline) {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    break;
                                }
                                
                                results.clear();
                                for (int i = 0; i < numStreams; i++) {
                                    List<StreamEntry> entries = streamStore.xread(keys[i], ids[i]);
                                    results.add(new StreamResult(keys[i], entries));
                                }
                                
                                hasData = false;
                                for (StreamResult r : results) {
                                    if (!r.entries.isEmpty()) {
                                        hasData = true;
                                        break;
                                    }
                                }
                                if (hasData) {
                                    sendXReadMultipleResponse(out, results);
                                    continue;
                                }
                            }
                            
                            sendNullBulkString(out);
                        }
                        else if(blockMs == 0){
                            while (true) {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    break;
                                }
                                
                                results.clear();
                                for (int i = 0; i < numStreams; i++) {
                                    List<StreamEntry> entries = streamStore.xread(keys[i], ids[i]);
                                    results.add(new StreamResult(keys[i], entries));
                                }
                                
                                hasData = false;
                                for (StreamResult r : results) {
                                    if (!r.entries.isEmpty()) {
                                        hasData = true;
                                        break;
                                    }
                                }
                                
                                if (hasData) {
                                    sendXReadMultipleResponse(out, results);
                                    continue;
                                }
                            }
                        } else {
                            sendXReadMultipleResponse(out, results);
                        }
                    }
                    default -> sendError(out, "unknown command");
                }
            
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

    private static void initiateReplicaHandshake() {
        new Thread(() -> {
            try {
                System.out.println("Connecting to master at " + masterHost + ":" + masterPort);
                Socket masterSocket = new Socket(masterHost, masterPort);
                OutputStream masterOut = masterSocket.getOutputStream();
                InputStream masterIn = masterSocket.getInputStream();
                BufferedReader masterReader = new BufferedReader(
                    new InputStreamReader(masterIn, StandardCharsets.UTF_8)
                );
                
                // Step 1: Send PING command as RESP array: *1\r\n$4\r\nPING\r\n
                String pingCommand = "*1\r\n$4\r\nPING\r\n";
                masterOut.write(pingCommand.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent PING to master");
                
                // Read PING response (expecting +PONG\r\n)
                String pingResponse = masterReader.readLine();
                System.out.println("Received PING response: " + pingResponse);
                
                // Step 2: Send REPLCONF listening-port <PORT>
                String port = String.valueOf(masterPort); // Using the port this replica is listening on
                // We need to get the actual port this server is listening on
                // For now, we'll need to pass it to this method
                String replconfPort = buildReplconfListeningPort();
                masterOut.write(replconfPort.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent REPLCONF listening-port");
                
                // Read REPLCONF response (expecting +OK\r\n)
                String replconfResponse1 = masterReader.readLine();
                System.out.println("Received REPLCONF response: " + replconfResponse1);
                
                // Step 3: Send REPLCONF capa psync2
                String replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                masterOut.write(replconfCapa.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent REPLCONF capa psync2");
                
                // Read REPLCONF response (expecting +OK\r\n)
                String replconfResponse2 = masterReader.readLine();
                System.out.println("Received REPLCONF response: " + replconfResponse2);
                
                // Step 4: Send PSYNC ? -1
                String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                masterOut.write(psyncCommand.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent PSYNC ? -1");
                
                // Read PSYNC response (expecting +FULLRESYNC <REPL_ID> 0\r\n)
                String psyncResponse = masterReader.readLine();
                System.out.println("Received PSYNC response: " + psyncResponse);
                
            } catch (IOException e) {
                System.err.println("Failed to connect to master: " + e.getMessage());
            }
        }).start();
    }
    
    private static String buildReplconfListeningPort() {
        // We need to get the port this replica is listening on
        // For simplicity, we'll store it as a static variable
        String portStr = String.valueOf(replicaListeningPort);
        int portLen = portStr.length();
        
        // *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$<len>\r\n<port>\r\n
        return "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + 
            portLen + "\r\n" + portStr + "\r\n";
    }

}

class TransactionState {
    private boolean inTransaction = false;
    private List<QueuedCommand> commandQueue = new ArrayList<>();
    
    public boolean isInTransaction() {
        return inTransaction;
    }
    
    public void startTransaction() {
        inTransaction = true;
        commandQueue.clear();
    }
    
    public void queueCommand(String command, String[] args) {
        commandQueue.add(new QueuedCommand(command, args));
    }
    
    public List<QueuedCommand> getCommandQueue() {
        return new ArrayList<>(commandQueue);  // Return copy
    }
    
    public void endTransaction() {
        inTransaction = false;
        commandQueue.clear();
    }
}

class QueuedCommand {
    final String command;
    final String[] args;
    
    QueuedCommand(String command, String[] args) {
        this.command = command;
        this.args = args.clone();  // Defensive copy
    }
}