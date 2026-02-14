import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XreadCommandHandler extends BaseCommandHandler {
    private final StreamStore streamStore;
    
    public XreadCommandHandler(StreamStore streamStore) {
        this.streamStore = streamStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 4) {
            sendError(out, "XREAD requires STREAMS keyword and at least one key-id pair");
            return;
        }
        
        long blockMs = 0;
        int argsStartIdx = 2;
        
        // Check for BLOCK option
        if (args.length >= 4 && args[1].equalsIgnoreCase("BLOCK")) {
            try {
                blockMs = Long.parseLong(args[2]);
                argsStartIdx = 4;
            } catch (NumberFormatException e) {
                sendError(out, "BLOCK timeout must be a number");
                return;
            }
        }
        
        // Check for STREAMS keyword
        if (!args[argsStartIdx - 1].equalsIgnoreCase("STREAMS")) {
            sendError(out, "XREAD requires STREAMS keyword");
            return;
        }
        
        int argsAfterStreams = args.length - argsStartIdx;
        if (argsAfterStreams % 2 != 0) {
            sendError(out, "Unbalanced XREAD STREAMS list");
            return;
        }
        
        int numStreams = argsAfterStreams / 2;
        String[] keys = new String[numStreams];
        String[] ids = new String[numStreams];
        
        for (int i = 0; i < numStreams; i++) {
            keys[i] = args[argsStartIdx + i];
            ids[i] = args[argsStartIdx + numStreams + i];
        }
        
        List<StreamResult> results = new ArrayList<>();
        for (int i = 0; i < numStreams; i++) {
            List<StreamEntry> entries = streamStore.xread(keys[i], ids[i]);
            results.add(new StreamResult(keys[i], entries));
        }
        
        boolean hasData = false;
        for (StreamResult r : results) {
            if (!r.entries.isEmpty()) {
                hasData = true;
                break;
            }
        }
        
        if (hasData) {
            sendXReadMultipleResponse(out, results);
        } else if (blockMs > 0) {
            long deadline = System.currentTimeMillis() + blockMs;
            
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                }
                
                results.clear();
                for (int i = 0; i < numStreams; i++) {
                    List<StreamEntry> entries = streamStore.xread(keys[i], ids[i]);
                    results.add(new StreamResult(keys[i], entries));
                }
                
                hasData = false;
                for (StreamResult r : results) {
                    if (!r.entries.isEmpty()) {
                        hasData = true;
                        break;
                    }
                }
                if (hasData) {
                    sendXReadMultipleResponse(out, results);
                    return;
                }
            }
            
            sendNullBulkString(out);
        } else if (blockMs == 0) {
            while (true) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                }
                
                results.clear();
                for (int i = 0; i < numStreams; i++) {
                    List<StreamEntry> entries = streamStore.xread(keys[i], ids[i]);
                    results.add(new StreamResult(keys[i], entries));
                }
                
                hasData = false;
                for (StreamResult r : results) {
                    if (!r.entries.isEmpty()) {
                        hasData = true;
                        break;
                    }
                }
                
                if (hasData) {
                    sendXReadMultipleResponse(out, results);
                    return;
                }
            }
        } else {
            sendXReadMultipleResponse(out, results);
        }
    }
    
    private void sendXReadMultipleResponse(OutputStream out, List<StreamResult> results) throws IOException {
        // Send array of streams
        out.write(("*" + results.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        
        for (StreamResult result : results) {
            // Each stream: [key, [entries]]
            out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
            
            // Stream key
            out.write(("$" + result.key.length() + "\r\n" + result.key + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            
            // Entries
            sendStreamEntries(out, result.entries);
        }
        out.flush();
    }
    
    private void sendStreamEntries(OutputStream out, List<StreamEntry> entries) throws IOException {
        out.write(("*" + entries.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (StreamEntry entry : entries) {
            out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
            
            String id = entry.getId();
            out.write(("$" + id.length() + "\r\n" + id + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            Map<String, String> fields = entry.getFields();
            int fieldCount = fields.size() * 2;
            out.write(("*" + fieldCount + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            for (Map.Entry<String, String> field : fields.entrySet()) {
                String fieldName = field.getKey();
                String fieldValue = field.getValue();
                out.write(("$" + fieldName.length() + "\r\n" + fieldName + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.write(("$" + fieldValue.length() + "\r\n" + fieldValue + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
    }
    
    @Override
    public String getCommandName() {
        return "XREAD";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // XREAD only reads data
    }
}