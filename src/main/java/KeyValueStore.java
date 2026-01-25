import java.util.concurrent.ConcurrentHashMap;

// Thread-safe key-value store
public class KeyValueStore {
    private final ConcurrentHashMap<String, String> store;

    public KeyValueStore() {
        store = new ConcurrentHashMap<>();
    }

    public void set(String key, String value) {
        store.put(key, value);
    }

    public String get(String key) {
        return store.get(key);
    }

    public boolean exists(String key) {
        return store.containsKey(key);
    }
}
