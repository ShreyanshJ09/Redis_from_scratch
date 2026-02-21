import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class WaitCommandHandler extends BaseCommandHandler {
    private final List<OutputStream> connectedReplicas;
    private final ReplicationTracker replicationTracker;
    
    public WaitCommandHandler(List<OutputStream> connectedReplicas,
                        ReplicationTracker replicationTracker) {
        this.connectedReplicas = connectedReplicas;
        this.replicationTracker = replicationTracker;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
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
        
        int ackedReplicas = executeWait(numReplicas, timeoutMs);
        
        sendInteger(out, ackedReplicas);
    }
    
    private int executeWait(int numReplicas, long timeoutMs) {
        int replicaCount;
        synchronized (connectedReplicas) {
            replicaCount = connectedReplicas.size();
        }
        
        if (replicaCount == 0) {
            return 0;
        }

        if (!replicationTracker.hasPendingWrites()) {
            return replicaCount;
        }

        return waitForReplicaAcknowledgments(numReplicas, timeoutMs, replicaCount);
    }
    
    /**
     * Stage 3: Wait for replicas to acknowledge write commands.
     * 
     * This method will:
     * 1. Send REPLCONF GETACK * to all replicas
     * 2. Collect ACK responses with offsets
     * 3. Count how many replicas have caught up to the latest offset
     * 4. Return when either:
     *    - Required number of replicas have ACKed, OR
     *    - Timeout expires
     * 
     * @param numReplicas minimum number of replicas needed
     * @param timeoutMs maximum wait time
     * @param totalReplicas total number of connected replicas
     * @return number of replicas that acknowledged
     */
    private int waitForReplicaAcknowledgments(int numReplicas, long timeoutMs, int totalReplicas) {
        // TODO: Stage 3 implementation
        // For now, return the replica count (Stage 2 behavior)
        // 
        // Stage 3 pseudocode:
        // 1. Get current master offset: long expectedOffset = replicationTracker.getCurrentOffset();
        // 2. Send REPLCONF GETACK * to all replicas
        // 3. Wait for ACK responses with timeout
        // 4. Count replicas where ackOffset >= expectedOffset
        // 5. Mark writes as no longer pending: replicationTracker.clearPendingWrites();
        // 6. Return count
        
        return totalReplicas;
    }
    
    @Override
    public String getCommandName() {
        return "WAIT";
    }
}