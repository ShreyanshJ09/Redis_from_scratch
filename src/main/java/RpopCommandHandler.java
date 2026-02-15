import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class RpopCommandHandler extends BaseCommandHandler {
    private final ListStore listStore;
    
    public RpopCommandHandler(ListStore listStore) {
        this.listStore = listStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "RPOP requires a key");
            return;
        }
        
        String key = args[1];
        
        // Check if count argument is provided
        if (args.length >= 3) {
            try {
                int count = Integer.parseInt(args[2]);
                List<String> values = listStore.rpop(key, count);
                
                if (values.isEmpty()) {
                    sendNullBulkString(out);
                } else {
                    sendArray(out, values);
                }
            } catch (NumberFormatException e) {
                sendError(out, "count must be a number");
            }
        } else {
            // Single pop
            String value = listStore.rpop(key);
            
            if (value == null) {
                sendNullBulkString(out);
            } else {
                sendBulkString(out, value);
            }
        }
    }
    
    @Override
    public String getCommandName() {
        return "RPOP";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true; // RPOP modifies data
    }
}