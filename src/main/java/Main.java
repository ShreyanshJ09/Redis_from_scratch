import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class Main {

    private static final KeyValueStore keyValueStore = new KeyValueStore();
    private static final ListStore listStore = new ListStore();
    private static final StreamStore streamStore = new StreamStore();
    
    // Store connected replicas for command propagation
    private static final List<OutputStream> connectedReplicas = new ArrayList<>();
    
    // Command registry
    private static CommandRegistry commandRegistry;
    
    // Thread-local transaction context for each client
    private static final ThreadLocal<TransactionContext> transactionContext =
        ThreadLocal.withInitial(TransactionContext::new);
    
    private static String serverRole = "master";
    private static String masterHost = null;
    private static int masterPort = 0;
    private static int replicaListeningPort = 6379;
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final int MASTER_REPL_OFFSET = 0;
    
    // Empty RDB file
    private static final byte[] EMPTY_RDB_FILE = hexStringToByteArray(
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    );
    
    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
    
    public static void main(String[] args) throws IOException {
        int port = 6379;
        
        // Parse command line arguments
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
        
        // Initialize command registry
        initializeCommandRegistry();
        
        // Start expiry thread for blocked clients
        startExpiryThread();
        
        // Start server
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Redis-like server running on port " + port);
        
        // If this is a replica, initiate handshake with master
        if (serverRole.equals("slave")) {
            initiateReplicaHandshake();
        }
        
        // Accept client connections
        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Client connected");
            new Thread(() -> handleClient(client)).start();
        }
    }
    
    /**
     * Initialize and register all command handlers
     */
    private static void initializeCommandRegistry() {
        commandRegistry = new CommandRegistry();
        
        // Register server/utility commands (READ)
        commandRegistry.register(new PingCommandHandler());
        commandRegistry.register(new EchoCommandHandler());
        commandRegistry.register(new InfoCommandHandler(serverRole, MASTER_REPLID, MASTER_REPL_OFFSET));
        
        // Register key-value commands
        commandRegistry.register(new SetCommandHandler(keyValueStore));  // WRITE
        commandRegistry.register(new GetCommandHandler(keyValueStore));  // READ
        commandRegistry.register(new IncrCommandHandler(keyValueStore)); // WRITE
        commandRegistry.register(new ExistsCommandHandler(keyValueStore, listStore, streamStore)); // READ
        commandRegistry.register(new TypeCommandHandler(keyValueStore, listStore, streamStore));   // READ
        
        // Register list commands
        commandRegistry.register(new RpushCommandHandler(listStore));  // WRITE
        commandRegistry.register(new LpushCommandHandler(listStore));  // WRITE
        commandRegistry.register(new LpopCommandHandler(listStore));   // WRITE
        commandRegistry.register(new RpopCommandHandler(listStore));   // WRITE
        commandRegistry.register(new LrangeCommandHandler(listStore)); // READ
        commandRegistry.register(new LlenCommandHandler(listStore));   // READ
        
        // Register stream commands
        commandRegistry.register(new XaddCommandHandler(streamStore));   // WRITE
        commandRegistry.register(new XrangeCommandHandler(streamStore)); // READ
        commandRegistry.register(new XreadCommandHandler(streamStore));  // READ
        
        // Register transaction commands (Special handling - see handleClient)
        // Note: We can't pass transactionContext here as it's thread-local
        // Transaction commands will be handled specially in handleClient
        
        // Register replication commands (READ - these are control commands)
        commandRegistry.register(new ReplconfCommandHandler(connectedReplicas, serverRole));
        commandRegistry.register(new PsyncCommandHandler(MASTER_REPLID, EMPTY_RDB_FILE, connectedReplicas));
    }

    private static void handleClient(Socket client) {
        try (
            InputStream in = client.getInputStream();
            OutputStream out = client.getOutputStream();
        ) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8)
            );
            
            TransactionContext txContext = transactionContext.get();

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
                
                // Handle transaction commands specially
                if (command.equals("MULTI")) {
                    MultiCommandHandler multiHandler = new MultiCommandHandler(txContext);
                    multiHandler.execute(args, out);
                    continue;
                }
                
                if (command.equals("EXEC")) {
                    ExecCommandHandler execHandler = new ExecCommandHandler(txContext, commandRegistry);
                    execHandler.execute(args, out);
                    continue;
                }
                
                if (command.equals("DISCARD")) {
                    DiscardCommandHandler discardHandler = new DiscardCommandHandler(txContext);
                    discardHandler.execute(args, out);
                    continue;
                }
                
                // If in transaction, queue the command
                if (txContext.isInTransaction()) {
                    txContext.queueCommand(command, args);
                    sendSimpleString(out, "QUEUED");
                    continue;
                }
                
                // Execute command using registry
                boolean executed = commandRegistry.executeCommand(command, args, out);
                
                if (!executed) {
                    sendError(out, "unknown command");
                    continue;
                }
                
                // Propagate write commands to replicas
                if (serverRole.equals("master") && commandRegistry.isWriteCommand(command)) {
                    propagateCommandToReplicas(args);
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected");
        } finally {
            // Clean up thread-local to prevent memory leaks
            transactionContext.remove();
        }
    }
    
    private static void sendSimpleString(OutputStream out, String msg) throws IOException {
        out.write(("+" + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    private static void sendError(OutputStream out, String msg) throws IOException {
        out.write(("-ERR " + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    private static void propagateCommandToReplicas(String[] args) {
        // Build RESP array for the command
        StringBuilder respArray = new StringBuilder();
        respArray.append("*").append(args.length).append("\r\n");
        
        for (String arg : args) {
            respArray.append("$").append(arg.length()).append("\r\n");
            respArray.append(arg).append("\r\n");
        }
        
        byte[] commandBytes = respArray.toString().getBytes(StandardCharsets.UTF_8);
        
        // Send to all connected replicas
        synchronized (connectedReplicas) {
            System.out.println("Propagating command to " + connectedReplicas.size() + " replica(s): " + args[0]);
            
            List<OutputStream> failedReplicas = new ArrayList<>();
            
            for (OutputStream replica : connectedReplicas) {
                try {
                    replica.write(commandBytes);
                    replica.flush();
                    System.out.println("Successfully propagated to replica");
                } catch (IOException e) {
                    System.err.println("Failed to propagate to replica: " + e.getMessage());
                    failedReplicas.add(replica);
                }
            }
            
            // Remove failed replicas
            connectedReplicas.removeAll(failedReplicas);
        }
    }
    
    private static void startExpiryThread() {
        new Thread(() -> {
            while (true) {
                List<BlockedClient> expired = listStore.collectExpiredBlockedClients();

                for (BlockedClient client : expired) {
                    try {
                        sendNullBulkString(client.out);
                    } catch (IOException ignored) {}
                }
                try { Thread.sleep(10); } catch (Exception ignored) {}
            }
        }).start();
    }
    
    private static void sendNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static void initiateReplicaHandshake() {
        new Thread(() -> {
            try {
                System.out.println("Connecting to master at " + masterHost + ":" + masterPort);
                Socket masterSocket = new Socket(masterHost, masterPort);
                OutputStream masterOut = masterSocket.getOutputStream();
                InputStream masterIn = masterSocket.getInputStream();
                
                // Use raw InputStream for handshake to avoid buffering issues
                // Step 1: Send PING command
                String pingCommand = "*1\r\n$4\r\nPING\r\n";
                masterOut.write(pingCommand.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent PING to master");
                
                String pingResponse = readLine(masterIn);
                System.out.println("Received PING response: " + pingResponse);
                
                // Step 2: Send REPLCONF listening-port
                String replconfPort = buildReplconfListeningPort();
                masterOut.write(replconfPort.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent REPLCONF listening-port");
                
                String replconfResponse1 = readLine(masterIn);
                System.out.println("Received REPLCONF response: " + replconfResponse1);
                
                // Step 3: Send REPLCONF capa psync2
                String replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                masterOut.write(replconfCapa.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent REPLCONF capa psync2");
                
                String replconfResponse2 = readLine(masterIn);
                System.out.println("Received REPLCONF response: " + replconfResponse2);
                
                // Step 4: Send PSYNC ? -1
                String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                masterOut.write(psyncCommand.getBytes(StandardCharsets.UTF_8));
                masterOut.flush();
                System.out.println("Sent PSYNC ? -1");
                
                String psyncResponse = readLine(masterIn);
                System.out.println("Received PSYNC response: " + psyncResponse);
                
                // Step 5: Receive RDB file
                String rdbHeader = readLine(masterIn);
                System.out.println("Received RDB header: " + rdbHeader);
                
                if (rdbHeader.startsWith("$")) {
                    int rdbLength = Integer.parseInt(rdbHeader.substring(1));
                    System.out.println("RDB file length: " + rdbLength + " bytes");
                    
                    // Read the RDB file contents (binary data) directly from InputStream
                    byte[] rdbData = new byte[rdbLength];
                    int totalRead = 0;
                    while (totalRead < rdbLength) {
                        int bytesRead = masterIn.read(rdbData, totalRead, rdbLength - totalRead);
                        if (bytesRead == -1) {
                            throw new IOException("Unexpected end of stream while reading RDB file");
                        }
                        totalRead += bytesRead;
                    }
                    System.out.println("Received RDB file: " + totalRead + " bytes");
                }
                
                // Step 6: Now create BufferedReader for command processing
                System.out.println("Replica ready to receive commands from master");
                BufferedReader masterReader = new BufferedReader(
                    new InputStreamReader(masterIn, StandardCharsets.UTF_8)
                );
                processCommandsFromMaster(masterReader, masterOut);
                
            } catch (IOException e) {
                System.err.println("Failed to connect to master: " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }
    
    /**
     * Read a line from InputStream (without BufferedReader to avoid buffering issues)
     */
    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    break;
                }
            }
            line.append((char) b);
        }
        return line.toString();
    }
    
    /**
     * Continuously read and process commands from the master
     */
    private static void processCommandsFromMaster(BufferedReader reader, OutputStream out) throws IOException {
        long replicationOffset = 0;  // Track bytes processed
        
        while (true) {
            // Mark the position to calculate command bytes
            ByteArrayOutputStream commandBuffer = new ByteArrayOutputStream();
            
            String line = reader.readLine();
            if (line == null) {
                System.out.println("Master connection closed");
                break;
            }
            
            // Write to buffer for byte counting
            commandBuffer.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            if (!line.startsWith("*")) {
                System.err.println("Invalid RESP from master: " + line);
                continue;
            }
            
            int argCount = Integer.parseInt(line.substring(1));
            String[] args = new String[argCount];
            
            for (int i = 0; i < argCount; i++) {
                String lengthLine = reader.readLine(); // $length
                commandBuffer.write((lengthLine + "\r\n").getBytes(StandardCharsets.UTF_8));
                
                args[i] = reader.readLine();
                commandBuffer.write((args[i] + "\r\n").getBytes(StandardCharsets.UTF_8));
            }
            
            String command = args[0].toUpperCase();
            byte[] commandBytes = commandBuffer.toByteArray();
            int commandByteLength = commandBytes.length;
            
            System.out.println("Replica received command from master: " + command + 
                             " (args: " + argCount + ", bytes: " + commandByteLength + ")");
            
            // Check if this is REPLCONF GETACK
            if (command.equals("REPLCONF") && args.length >= 2 && args[1].equalsIgnoreCase("GETACK")) {
                // Respond with current offset (BEFORE processing this command)
                String offsetStr = String.valueOf(replicationOffset);
                String response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                                 offsetStr.length() + "\r\n" + offsetStr + "\r\n";
                out.write(response.getBytes(StandardCharsets.UTF_8));
                out.flush();
                System.out.println("Sent REPLCONF ACK " + replicationOffset);
                
                // Now add this GETACK command's bytes to offset
                replicationOffset += commandByteLength;
            } else {
                // Process other commands silently (no response to master)
                try {
                    // Use a NullOutputStream to discard responses
                    OutputStream nullOut = new ByteArrayOutputStream();
                    commandRegistry.executeCommand(command, args, nullOut);
                    System.out.println("Replica executed: " + command);
                } catch (Exception e) {
                    System.err.println("Error executing command on replica: " + e.getMessage());
                }
                
                // Add command bytes to offset
                replicationOffset += commandByteLength;
            }
            
            System.out.println("Current replication offset: " + replicationOffset);
        }
    }
    
    private static String buildReplconfListeningPort() {
        String portStr = String.valueOf(replicaListeningPort);
        int portLen = portStr.length();
        
        return "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + 
               portLen + "\r\n" + portStr + "\r\n";
    }
}