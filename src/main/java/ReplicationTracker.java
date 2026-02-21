import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;


public class ReplicationTracker {
    private volatile boolean hasPendingWrites = false;
    private volatile long currentOffset = 0;
    
    private final ConcurrentHashMap<OutputStream, Long> replicaOffsets = new ConcurrentHashMap<>();
    

    public synchronized boolean hasPendingWrites() {
        return hasPendingWrites;
    }

    public synchronized void markWriteSent() {
        hasPendingWrites = true;
    }
    

    public synchronized void clearPendingWrites() {
        hasPendingWrites = false;
    }
    

    public synchronized long getCurrentOffset() {
        return currentOffset;
    }
    

    public synchronized void addToOffset(long bytes) {
        currentOffset += bytes;
    }
    

    public void recordReplicaOffset(OutputStream replica, long offset) {
        replicaOffsets.put(replica, offset);
    }
    

    public int countReplicasAtOffset(long expectedOffset, java.util.List<OutputStream> replicas) {
        int count = 0;
        for (OutputStream replica : replicas) {
            Long replicaOffset = replicaOffsets.get(replica);
            if (replicaOffset != null && replicaOffset >= expectedOffset) {
                count++;
            }
        }
        return count;
    }
    

    public void clearReplicaOffsets() {
        replicaOffsets.clear();
    }
    
    public synchronized void reset() {
        hasPendingWrites = false;
        currentOffset = 0;
        replicaOffsets.clear();
    }
}