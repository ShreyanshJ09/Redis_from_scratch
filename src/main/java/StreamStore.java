import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStore {
    // Map of stream key -> list of entries
    private final ConcurrentHashMap<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();

    /**
     * Add an entry to a stream (XADD command)
     * @param key The stream key
     * @param entryId The entry ID (e.g., "1526919030474-0")
     * @param fields Map of field-value pairs
     * @return The entry ID that was added
     * @throws IllegalArgumentException if the ID is invalid
     */
    public synchronized String xadd(String key, String entryId, Map<String, String> fields) throws IllegalArgumentException {
        // Parse the provided entry ID
        EntryId newId;
        try {
            newId = new EntryId(entryId);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid entry ID format");
        }

        // Check if ID is 0-0 (always invalid)
        EntryId zeroId = new EntryId(0, 0);
        if (newId.getMillisecondsTime() == 0 && newId.getSequenceNumber() == 0) {
            throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");
        }

        // Get or create the stream
        List<StreamEntry> stream = streams.computeIfAbsent(key, k -> new ArrayList<>());
        
        // Validate that new ID is greater than the last entry's ID
        if (!stream.isEmpty()) {
            StreamEntry lastEntry = stream.get(stream.size() - 1);
            EntryId lastId = new EntryId(lastEntry.getId());
            
            if (!newId.isGreaterThan(lastId)) {
                throw new IllegalArgumentException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        } else {
            // Stream is empty, ID must be greater than 0-0
            if (!newId.isGreaterThan(zeroId)) {
                throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");
            }
        }
        
        // Create and add the new entry
        StreamEntry entry = new StreamEntry(entryId, fields);
        stream.add(entry);
        
        return entryId;
    }

    /**
     * Check if a key exists as a stream
     */
    public boolean exists(String key) {
        return streams.containsKey(key);
    }

    /**
     * Get a stream by key (for future use)
     */
    public List<StreamEntry> getStream(String key) {
        return streams.get(key);
    }
}

/**
 * Represents a single entry in a Redis stream
 */
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

/**
 * Represents a parsed entry ID with millisecondsTime and sequenceNumber
 */
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