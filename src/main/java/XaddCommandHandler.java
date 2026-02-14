import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class XaddCommandHandler extends BaseCommandHandler {
    private final StreamStore streamStore;
    
    public XaddCommandHandler(StreamStore streamStore) {
        this.streamStore = streamStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 4) {
            sendError(out, "XADD requires key, entryId, and at least one field-value pair");
            return;
        }
        
        String key = args[1];
        String entryId = args[2];
        
        // Parse field-value pairs
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 3; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                sendError(out, "XADD requires field-value pairs");
                return;
            }
            fields.put(args[i], args[i + 1]);
        }
        
        try {
            String actualEntryId = streamStore.xadd(key, entryId, fields);
            sendBulkString(out, actualEntryId);
        } catch (IllegalArgumentException e) {
            sendError(out, e.getMessage());
        }
    }
    
    @Override
    public String getCommandName() {
        return "XADD";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true; // XADD modifies data
    }
}