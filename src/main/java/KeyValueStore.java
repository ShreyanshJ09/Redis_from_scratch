import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {
    private final ConcurrentHashMap<String, ValueWithExpiry> store;


    public KeyValueStore() {
        store = new ConcurrentHashMap<>();
    }

    public void set(String key, String value, long expiryMillis) {
        long now = System.currentTimeMillis();
        long expiryTime;
        if (expiryMillis > Long.MAX_VALUE - now) {
            expiryTime = Long.MAX_VALUE;
        } else {
            expiryTime = now + expiryMillis;
        }
        store.put(key, new ValueWithExpiry(value, expiryTime));
    }

    public String get(String key) {
        ValueWithExpiry v = store.get(key);
        if (v == null) return null;
        if (v.isExpired()) {
            store.remove(key);
            return null;
        }
        return v.getValue();
    }

    public synchronized long increment(String key) {
        ValueWithExpiry v = store.get(key);
        
        if (v == null) {
            store.put(key, new ValueWithExpiry("1", Long.MAX_VALUE));
            return 1;
        }
        
        if (v.isExpired()) {
            store.remove(key);
            store.put(key, new ValueWithExpiry("1", Long.MAX_VALUE));
            return 1;
        }
        
        String currentValue = v.getValue();
        long currentExpiry = v.getExpiryTime();
        
        try {
            long numValue = Long.parseLong(currentValue);
            
            if (numValue == Long.MAX_VALUE) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            
            long newValue = numValue + 1;
            
            store.put(key, new ValueWithExpiry(Long.toString(newValue), currentExpiry));
            
            return newValue;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
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
    public java.util.List<String> getAllKeys() {
        java.util.List<String> keys = new java.util.ArrayList<>();
        
        for (String key : store.keySet()) {
            ValueWithExpiry v = store.get(key);
            if (v != null && !v.isExpired()) {
                keys.add(key);
            }
        }
        
        return keys;
    }
}