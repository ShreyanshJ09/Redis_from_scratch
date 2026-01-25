import java.util.ArrayList;
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

    public int lpush(String key, String[] values, int startIndex) {
        List<String> list = lists.computeIfAbsent(
            key,
            k -> new CopyOnWriteArrayList<>()
        );

        // Insert in reverse order
        for (int i = startIndex; i < values.length; i++) {
            list.add(0, values[i]);
        }

        return list.size();
    }


    public List<String> lrange(String key, int start, int stop) {
        List<String> list = lists.get(key);

        if (list == null) {
            return List.of();
        }

        int size = list.size();

        if (start < 0) {
            start = size + start;
        }
        if (stop < 0) {
            stop = size + stop;
        }

        if (start < 0) start = 0;
        if (stop < 0) stop = 0;

        if (start >= size || start > stop) {
            return List.of();
        }

        stop = Math.min(stop, size - 1);

        List<String> result = new ArrayList<>();
        for (int i = start; i <= stop; i++) {
            result.add(list.get(i));
        }

        return result;
    }

    public int llen(String key) {
        List<String> list = lists.get(key);
        return list == null ? 0 : list.size();
    }



    public boolean exists(String key) {
        return lists.containsKey(key);
    }
}
