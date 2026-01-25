import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ListStore {

    private final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();

    // RPUSH key value
    public int rpush(String key, String value) {
        List<String> list = lists.computeIfAbsent(
            key,
            k -> new CopyOnWriteArrayList<>()
        );
        list.add(value);
        return list.size();
    }

    public boolean exists(String key) {
        return lists.containsKey(key);
    }
}
