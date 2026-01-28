// ConcurrencyTest.java
import java.util.concurrent.*;
import java.util.List;

public class ConcurrencyTest {
    public static void main(String[] args) throws Exception {
        ListStore store = new ListStore();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        
        // Test 1: 100 threads each push 10 values
        System.out.println("Test 1: Concurrent LPUSH");
        CountDownLatch latch = new CountDownLatch(100);
        
        for (int i = 0; i < 100; i++) {
            final int threadId = i;
            executor.submit(() -> {
                for (int j = 0; j < 10; j++) {
                    store.lpush("test", "value-" + threadId + "-" + j);
                }
                latch.countDown();
            });
        }
        
        latch.await();
        int size = store.llen("test");
        System.out.println("Expected: 1000, Got: " + size);
        System.out.println(size == 1000 ? "✅ PASS" : "❌ FAIL");
        
        // Test 2: Concurrent LPOP
        System.out.println("\nTest 2: Concurrent LPOP");
        CountDownLatch latch2 = new CountDownLatch(100);
        ConcurrentHashMap<String, Boolean> popped = new ConcurrentHashMap<>();
        
        for (int i = 0; i < 100; i++) {
            executor.submit(() -> {
                for (int j = 0; j < 10; j++) {
                    String val = store.lpop("test");
                    if (val != null) {
                        popped.put(val, true);
                    }
                }
                latch2.countDown();
            });
        }
        
        latch2.await();
        System.out.println("Unique values popped: " + popped.size());
        System.out.println("Remaining in list: " + store.llen("test"));
        System.out.println(popped.size() == 1000 && store.llen("test") == 0 ? "✅ PASS" : "❌ FAIL");
        
        executor.shutdown();
    }
}