import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.io.OutputStream;

public class ListStore {

    private final ConcurrentHashMap<String, List<String>> lists = new ConcurrentHashMap<>();
    private final Map<String, Queue<BlockedClient>> blockedClients = new ConcurrentHashMap<>();

    // RPUSH key value
    public synchronized WakeUpResult rpush(String key, String value) {
        Queue<BlockedClient> queue = blockedClients.get(key);
        long now = System.currentTimeMillis();

        // If someone is waiting → wake them
        if (queue != null) {
            while (!queue.isEmpty()) {
                BlockedClient client = queue.poll();
                if (client.expireAt <= now) {
                    continue;
                }
                return new WakeUpResult(client, value);
            }
        }

        // Otherwise push normally
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
        return null;
    }

    public synchronized WakeUpResult lpush(String key, String value) {
        Queue<BlockedClient> queue = blockedClients.get(key);
        long now = System.currentTimeMillis();

        // If someone is waiting → wake them
        if (queue != null) {
            while (!queue.isEmpty()) {
                BlockedClient client = queue.poll();
                if (client.expireAt <= now) {
                    continue;
                }
                return new WakeUpResult(client, value);
            }
        }

        // Otherwise push normally (at the LEFT/front)
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(0, value);
        return null;
    }

    public synchronized List<String> lrange(String key, int start, int stop) {
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

    public synchronized int llen(String key) {
        List<String> list = lists.get(key);
        return list == null ? 0 : list.size();
    }

    public synchronized String lpop(String key) {
        List<String> list = lists.get(key);

        if (list == null || list.isEmpty()) {
            return null;
        }

        String value = list.remove(0);

        if (list.isEmpty()) {
            lists.remove(key);
        }

        return value;
    }

    public synchronized List<String> lpop(String key, int count) {
        List<String> result = new ArrayList<>();
        List<String> list = lists.get(key);

        if (list == null || list.isEmpty() || count <= 0) {
            return result;
        }

        int actualCount = Math.min(count, list.size());

        for (int i = 0; i < actualCount; i++) {
            result.add(list.remove(0));
        }

        if (list.isEmpty()) {
            lists.remove(key);
        }

        return result;
    }

    public synchronized boolean exists(String key) {
        return lists.containsKey(key);
    }

    public synchronized List<BlockedClient> collectExpiredBlockedClients() {
        long now = System.currentTimeMillis();
        List<BlockedClient> expired = new ArrayList<>();

        for (Queue<BlockedClient> queue : blockedClients.values()) {
            while (!queue.isEmpty() && queue.peek().expireAt <= now) {
                expired.add(queue.poll());
            }
        }
        return expired;
    }
    
    public synchronized void blockClient(String key, OutputStream out, long timeoutMs) {
        long expireAt = System.currentTimeMillis() + timeoutMs;

        blockedClients
            .computeIfAbsent(key, k -> new ArrayDeque<>())
            .add(new BlockedClient(key, out, expireAt));
    }
}

class BlockedClient {
    String key;
    OutputStream out;
    long expireAt;

    BlockedClient(String key, OutputStream out, long expireAt) {
        this.key = key;
        this.out = out;
        this.expireAt = expireAt;
    }
}

class WakeUpResult {
    final BlockedClient client;
    final String value;

    WakeUpResult(BlockedClient client, String value) {
        this.client = client;
        this.value = value;
    }
}