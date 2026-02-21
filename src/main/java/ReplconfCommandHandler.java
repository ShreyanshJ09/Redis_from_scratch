import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ReplconfCommandHandler extends BaseCommandHandler {
    private final List<OutputStream> connectedReplicas;
    private final String serverRole;
    private final ReplicationTracker replicationTracker;
    
    public ReplconfCommandHandler(List<OutputStream> connectedReplicas, String serverRole,
                                  ReplicationTracker replicationTracker) {
        this.connectedReplicas = connectedReplicas;
        this.serverRole = serverRole;
        this.replicationTracker = replicationTracker;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "ERR wrong number of arguments for 'replconf' command");
            return;
        }
        
        String subcommand = args[1].toUpperCase();
        
        // Handle REPLCONF GETACK * (master receives from client, propagates to replicas)
        if (serverRole.equals("master") && subcommand.equals("GETACK")) {
            handleGetAckOnMaster(args, out);
        }
        // Handle REPLCONF ACK <offset> (master receives from replica)
        else if (serverRole.equals("master") && subcommand.equals("ACK")) {
            handleAckFromReplica(args, out);
        }
        // Handle other REPLCONF commands (listening-port, capa)
        else {
            sendSimpleString(out, "OK");
        }
    }
    
    private void handleGetAckOnMaster(String[] args, OutputStream clientOut) throws IOException {
        // Build the REPLCONF GETACK * command in RESP format
        String getAckCommand = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        byte[] commandBytes = getAckCommand.getBytes();
        
        List<OutputStream> replicasCopy;
        synchronized (connectedReplicas) {
            replicasCopy = List.copyOf(connectedReplicas);
        }
        
        System.out.println("Master propagating REPLCONF GETACK to " + replicasCopy.size() + " replica(s)");
        
        // Send GETACK to each replica
        for (OutputStream replicaOut : replicasCopy) {
            try {
                replicaOut.write(commandBytes);
                replicaOut.flush();
                System.out.println("Sent GETACK to replica");
            } catch (IOException e) {
                System.err.println("Failed to send GETACK to replica: " + e.getMessage());
            }
        }
        
        // Acknowledge to the client that command was sent
        sendSimpleString(clientOut, "OK");
    }
    
    private void handleAckFromReplica(String[] args, OutputStream out) throws IOException {
        // Master receives REPLCONF ACK <offset> from replica
        // Format: REPLCONF ACK <offset>
        
        if (args.length < 3) {
            System.err.println("Invalid REPLCONF ACK: missing offset");
            return;
        }
        
        String offsetStr = args[2];
        
        try {
            long offset = Long.parseLong(offsetStr);
            System.out.println("Master received ACK from replica: offset=" + offset);
            
            replicationTracker.recordReplicaOffset(out, offset);
        } catch (NumberFormatException e) {
            System.err.println("Invalid offset in REPLCONF ACK: " + offsetStr);
        }
    }
    
    @Override
    public String getCommandName() {
        return "REPLCONF";
    }
}