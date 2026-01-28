import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;

public class RaceConditionTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("Testing Race Condition 1: Timeout race");
        testTimeoutRace();
        
        Thread.sleep(1000);
        
        System.out.println("\nTesting Race Condition 2: Duplicate wake-up");
        testDuplicateWakeup();
    }
    
    static void testTimeoutRace() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        // Client blocks with 1 second timeout
        Future<?> blocker = executor.submit(() -> {
            try {
                Socket s = new Socket("localhost", 6379);
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();
                
                // BLPOP testkey 1
                String cmd = "*3\r\n$5\r\nBLPOP\r\n$7\r\ntestkey\r\n$1\r\n1\r\n";
                out.write(cmd.getBytes());
                out.flush();
                
                System.out.println("Client blocked, waiting for timeout...");
                Thread.sleep(1500); // Wait past timeout
                System.out.println("Client should have timed out by now");
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // Wait for client to block
        Thread.sleep(500);
        
        // RPUSH right at timeout boundary (race window!)
        Future<?> pusher = executor.submit(() -> {
            try {
                Thread.sleep(1050); // Push just after 1 second timeout
                
                Socket s = new Socket("localhost", 6379);
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();
                
                // RPUSH testkey value
                String cmd = "*3\r\n$5\r\nRPUSH\r\n$7\r\ntestkey\r\n$5\r\nvalue\r\n";
                out.write(cmd.getBytes());
                out.flush();
                
                byte[] response = new byte[1024];
                int len = in.read(response);
                System.out.println("RPUSH response: " + new String(response, 0, len).trim());
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        blocker.get();
        pusher.get();
        executor.shutdown();
    }
    
    static void testDuplicateWakeup() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch pushLatch = new CountDownLatch(2);
        
        // Blocking client
        Future<String> blocker = executor.submit(() -> {
            try {
                Socket s = new Socket("localhost", 6379);
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();
                
                // BLPOP testkey2 0 (infinite wait)
                String cmd = "*3\r\n$5\r\nBLPOP\r\n$8\r\ntestkey2\r\n$1\r\n0\r\n";
                out.write(cmd.getBytes());
                out.flush();
                
                ready.countDown();
                System.out.println("Client blocking...");
                
                // Read response
                byte[] response = new byte[1024];
                int len = in.read(response);
                String resp = new String(response, 0, len).trim();
                System.out.println("Blocker got: " + resp);
                
                return resp;
                
            } catch (Exception e) {
                e.printStackTrace();
                return "ERROR";
            }
        });
        
        ready.await(); // Wait for blocker to be ready
        Thread.sleep(100); // Ensure it's blocking
        
        // Two simultaneous RPUSH
        Callable<String> rpushTask = () -> {
            try {
                Socket s = new Socket("localhost", 6379);
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();
                
                pushLatch.countDown();
                pushLatch.await(); // Synchronize both threads
                
                // RPUSH testkey2 value
                String cmd = "*3\r\n$5\r\nRPUSH\r\n$8\r\ntestkey2\r\n$5\r\nvalue\r\n";
                out.write(cmd.getBytes());
                out.flush();
                
                byte[] response = new byte[1024];
                int len = in.read(response);
                String resp = new String(response, 0, len).trim();
                System.out.println("RPUSH response: " + resp);
                return resp;
                
            } catch (Exception e) {
                e.printStackTrace();
                return "ERROR";
            }
        };
        
        Future<String> push1 = executor.submit(rpushTask);
        Future<String> push2 = executor.submit(rpushTask);
        
        String blockerResp = blocker.get(3, TimeUnit.SECONDS);
        String push1Resp = push1.get();
        String push2Resp = push2.get();
        
        System.out.println("\n=== RESULTS ===");
        System.out.println("Blocker: " + blockerResp);
        System.out.println("Push1: " + push1Resp);
        System.out.println("Push2: " + push2Resp);
        
        // Check for bugs
        if (push1Resp.equals(":0") && push2Resp.equals(":0")) {
            System.out.println("❌ BUG: Both RPUSHs returned :0 (both woke same client!)");
        }
        
        executor.shutdown();
    }
}