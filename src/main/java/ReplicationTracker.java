public class ReplicationTracker {
    private volatile boolean hasPendingWrites = false;
    private volatile long currentOffset = 0;
    

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
    
    public synchronized void reset() {
        hasPendingWrites = false;
        currentOffset = 0;
    }
}