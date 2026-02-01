import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStore {
    private final ConcurrentHashMap<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();

    public synchronized String xadd(String key, String entryId, Map<String, String> fields) {
    String actualEntryId = entryId;

    if(entryId.equals("*")) {
        long timeMs = System.currentTimeMillis();
        actualEntryId = timeMs + "-0";
    }
    else if (entryId.contains("-*")) {
        String[] parts = entryId.split("-");
        long timeMs = Long.parseLong(parts[0]);

        List<StreamEntry> stream = streams.get(key);
        
        long sequence;
        if (stream == null || stream.isEmpty()) {
            if (timeMs == 0) {
                sequence = 1;
            } else {
                sequence = 0;
            }
        } else {
            StreamEntry lastEntry = stream.get(stream.size() - 1);
            String[] lastParts = lastEntry.getId().split("-");
            long lastTime = Long.parseLong(lastParts[0]);
            long lastSeq = Long.parseLong(lastParts[1]);
            
            if (lastTime == timeMs) {
                sequence = lastSeq + 1;
            } else {
                if (timeMs == 0) {
                    sequence = 1;
                } else {
                    sequence = 0;
                }
            }
        }
        
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
            if (!newId.isGreaterThan(zeroId)) {
                throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
            }
        }

    StreamEntry entry = new StreamEntry(actualEntryId, fields);
    stream.add(entry);
    
    return actualEntryId;
}

    public boolean exists(String key) {
        return streams.containsKey(key);
    }


    public List<StreamEntry> getStream(String key) {
        return streams.get(key);
    }

    public synchronized List<StreamEntry> xrange(String key, String startId, String endId) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null || stream.isEmpty()) {
            return new ArrayList<>();
        }
        
        EntryId start = parseRangeId(startId, true);
        EntryId end = parseRangeId(endId, false);
        
        List<StreamEntry> result = new ArrayList<>();
        for (StreamEntry entry : stream) {
            EntryId entryId = new EntryId(entry.getId());
            
            if (entryId.isGreaterThanOrEqual(start) && entryId.isLessThanOrEqual(end)) {
                result.add(entry);
            }
        }
        
        return result;
    }

    public synchronized List<StreamEntry> xread(String key, String startId) {
        List<StreamEntry> stream = streams.get(key);
        if (stream == null || stream.isEmpty()) {
            return new ArrayList<>();
        }
        
        EntryId start = new EntryId(startId);
        List<StreamEntry> result = new ArrayList<>();
        
        for (StreamEntry entry : stream) {
            EntryId entryId = new EntryId(entry.getId());
            
            if (entryId.equals(start)) {
                result.add(entry);
            }
        }
        
        return result;
    }

    private EntryId parseRangeId(String id, boolean isStart) {
        if (id.equals("-") && isStart) {
            return new EntryId(0, 0);
        }
        if (id.equals("+") && !isStart) {
            return new EntryId(Long.MAX_VALUE, Long.MAX_VALUE);
        }

        if (id.contains("-")) {
            return new EntryId(id);
        } else {
            // Missing sequence number
            long time = Long.parseLong(id);
            if (isStart) {
                return new EntryId(time, 0);  // Start defaults to 0
            } else {
                return new EntryId(time, Long.MAX_VALUE);  // End defaults to max
            }
        }
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

    public boolean isGreaterThanOrEqual(EntryId other) {
        return this.equals(other) || this.isGreaterThan(other);
    }

    public boolean isLessThanOrEqual(EntryId other) {
        return this.equals(other) || !this.isGreaterThan(other);
    }

    public boolean equals(EntryId other) {
        return this.millisecondsTime == other.millisecondsTime &&
            this.sequenceNumber == other.sequenceNumber;
    }

    @Override
    public String toString() {
        return millisecondsTime + "-" + sequenceNumber;
    }
}

class StreamResult {
    final String key;
    final List<StreamEntry> entries;
    
    StreamResult(String key, List<StreamEntry> entries) {
        this.key = key;
        this.entries = entries;
    }
}