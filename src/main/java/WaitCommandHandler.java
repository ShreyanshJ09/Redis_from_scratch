import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Handles the WAIT command for checking replica acknowledgments.
 * 
 * Format: WAIT <numreplicas> <timeout>
 * 
 * Stages:
 * 1. No replicas connected → return 0
 * 2. Replicas connected, no writes sent → return replica count
 * 3. Replicas connected, writes sent → check ACKs (to be implemented)
 */
public class WaitCommandHandler extends BaseCommandHandler {
    private final List<OutputStream> connectedReplicas;
    private final ReplicationTracker replicationTracker;
    
    public WaitCommandHandler(List<OutputStream> connectedReplicas, ReplicationTracker replicationTracker) {
        this.connectedReplicas = connectedReplicas;
        this.replicationTracker = replicationTracker;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // Parse arguments
        if (args.length < 3) {
            sendError(out, "ERR wrong number of arguments for 'wait' command");
            return;
        }
        
        int numReplicas;
        long timeoutMs;
        
        try {
            numReplicas = Integer.parseInt(args[1]);
            timeoutMs = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            sendError(out, "ERR value is not an integer or out of range");
            return;
        }
        
        // Execute WAIT logic
        int ackedReplicas = executeWait(numReplicas, timeoutMs);
        
        // Send response as RESP integer
        sendInteger(out, ackedReplicas);
    }
    
    /**
     * Core WAIT logic - returns number of replicas that have acknowledged.
     * 
     * @param numReplicas minimum number of replicas expected
     * @param timeoutMs maximum time to wait in milliseconds
     * @return actual number of replicas that acknowledged
     */
    private int executeWait(int numReplicas, long timeoutMs) {
        int replicaCount;
        synchronized (connectedReplicas) {
            replicaCount = connectedReplicas.size();
        }
        
        // Stage 1: No replicas connected
        if (replicaCount == 0) {
            return 0;
        }
        
        // Stage 2: Replicas connected, but no writes since last check
        if (!replicationTracker.hasPendingWrites()) {
            // All replicas are in sync (no writes to acknowledge)
            return replicaCount;
        }
        
        // Stage 3: Need to check replica acknowledgments (TODO)
        // This will be implemented in the next stage
        return waitForReplicaAcknowledgments(numReplicas, timeoutMs, replicaCount);
    }
    
    private int waitForReplicaAcknowledgments(int numReplicas, long timeoutMs, int totalReplicas) {

        long expectedOffset = replicationTracker.getCurrentOffset();
        
        List<OutputStream> replicasCopy;
        synchronized (connectedReplicas) {
            replicasCopy = new ArrayList<>(connectedReplicas);
        }
        
        if (replicasCopy.isEmpty()) {
            return 0;
        }
        
        sendGetAckToReplicas(replicasCopy);
        
        long deadline = System.currentTimeMillis() + timeoutMs;
        int ackedCount = 0;
        
        while (System.currentTimeMillis() < deadline) {
            // Count how many replicas have reached the expected offset
            ackedCount = replicationTracker.countReplicasAtOffset(expectedOffset, replicasCopy);
            
            // If we have enough replicas, return early
            if (ackedCount >= numReplicas) {
                break;
            }
            
            // Wait a bit before checking again (poll every 10ms)
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        // Final count (either we got enough, or timeout expired)
        ackedCount = replicationTracker.countReplicasAtOffset(expectedOffset, replicasCopy);
        
        // Clear pending writes flag
        replicationTracker.clearPendingWrites();
        
        return ackedCount;
    }
    
    private void sendGetAckToReplicas(List<OutputStream> replicas) {
        String getAckCommand = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        byte[] commandBytes = getAckCommand.getBytes();
        
        for (OutputStream replica : replicas) {
            try {
                replica.write(commandBytes);
                replica.flush();
            } catch (IOException e) {
                System.err.println("Failed to send GETACK to replica: " + e.getMessage());
            }
        }
    }
    
    @Override
    public String getCommandName() {
        return "WAIT";
    }
}