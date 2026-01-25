import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {
    private final ConcurrentHashMap<String, ValueWithExpiry> store;

    public KeyValueStore() {
        store = new ConcurrentHashMap<>();
    }

    // Set key with expiry (PX in ms or EX in seconds * 1000)
    public void set(String key, String value, long expiryMillis) {
        long now = System.currentTimeMillis();
        long expiryTime;
        if (expiryMillis > Long.MAX_VALUE - now) {
            expiryTime = Long.MAX_VALUE; // prevent overflow
        } else {
            expiryTime = now + expiryMillis;
        }
        store.put(key, new ValueWithExpiry(value, expiryTime));
    }

    public String get(String key) {
        ValueWithExpiry v = store.get(key);
        if (v == null) return null;
        if (v.isExpired()) {
            store.remove(key); // remove expired key immediately
            return null;
        }
        return v.getValue();
    }

    public boolean exists(String key) {
        ValueWithExpiry v = store.get(key);
        if (v == null) return false;
        if (v.isExpired()) {
            store.remove(key);
            return false;
        }
        return true;
    }
}
