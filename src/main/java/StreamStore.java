import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStore {
    // Map of stream key -> list of entries
    private final ConcurrentHashMap<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();

    public synchronized String xadd(String key, String entryId, Map<String, String> fields) {
    // Step 1: Check if we need to auto-generate sequence
    String actualEntryId = entryId;

    if(entryId.equals("*")) {
        long timeMs = System.currentTimeMillis();
        actualEntryId = timeMs + "-0";
    }
    else if (entryId.contains("-*")) {
        // Parse the time part
        String[] parts = entryId.split("-");
        long timeMs = Long.parseLong(parts[0]);
        
        // Get the stream
        List<StreamEntry> stream = streams.get(key);
        
        // Determine the sequence number
        long sequence;
        if (stream == null || stream.isEmpty()) {
            // Empty stream
            if (timeMs == 0) {
                sequence = 1;  // Special case: 0-* becomes 0-1
            } else {
                sequence = 0;  // Normal case: N-* becomes N-0
            }
        } else {
            // Stream has entries - check if last entry has same time
            StreamEntry lastEntry = stream.get(stream.size() - 1);
            String[] lastParts = lastEntry.getId().split("-");
            long lastTime = Long.parseLong(lastParts[0]);
            long lastSeq = Long.parseLong(lastParts[1]);
            
            if (lastTime == timeMs) {
                // Same time - increment sequence
                sequence = lastSeq + 1;
            } else {
                // Different time - start at 0
                if (timeMs == 0) {
                    sequence = 1;  // Special case
                } else {
                    sequence = 0;
                }
            }
        }
        
        // Build the actual ID
        actualEntryId = timeMs + "-" + sequence;
    }
    EntryId newId = new EntryId(actualEntryId);
    EntryId zeroId = new EntryId(0,0);
    List<StreamEntry> stream = streams.computeIfAbsent(key, k -> new ArrayList<>());
    if (!stream.isEmpty()) {
            StreamEntry lastEntry = stream.get(stream.size() - 1);
            EntryId lastId = new EntryId(lastEntry.getId());
            
            if (!newId.isGreaterThan(lastId)) {
                throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        } else {
            // Stream is empty, ID must be greater than 0-0
            if (!newId.isGreaterThan(zeroId)) {
                throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
            }
        }
    
    // Step 2: Now validate using the actual ID (not the one with *)
    
    
    // ... rest of validation logic ...
    
    // Step 3: Create entry with the ACTUAL id
    StreamEntry entry = new StreamEntry(actualEntryId, fields);
    stream.add(entry);
    
    return actualEntryId;  // Return the generated ID
}

    public boolean exists(String key) {
        return streams.containsKey(key);
    }


    public List<StreamEntry> getStream(String key) {
        return streams.get(key);
    }
}


class StreamEntry {
    private final String id;
    private final Map<String, String> fields;

    public StreamEntry(String id, Map<String, String> fields) {
        this.id = id;
        this.fields = new LinkedHashMap<>(fields); // Preserve insertion order
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getFields() {
        return fields;
    }
}


class EntryId {
    private final long millisecondsTime;
    private final long sequenceNumber;

    public EntryId(String id) {
        String[] parts = id.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid entry ID format");
        }
        this.millisecondsTime = Long.parseLong(parts[0]);
        this.sequenceNumber = Long.parseLong(parts[1]); 
    }

    public EntryId(long millisecondsTime, long sequenceNumber) {
        this.millisecondsTime = millisecondsTime;
        this.sequenceNumber = sequenceNumber;
    }

    public long getMillisecondsTime() {
        return millisecondsTime;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public boolean isGreaterThan(EntryId other) {
        if (this.millisecondsTime > other.millisecondsTime) {
            return true;
        }
        if (this.millisecondsTime == other.millisecondsTime) {
            return this.sequenceNumber > other.sequenceNumber;
        }
        return false;
    }

    @Override
    public String toString() {
        return millisecondsTime + "-" + sequenceNumber;
    }
}