import java.io.IOException;
import java.io.OutputStream;

public class IncrCommandHandler extends BaseCommandHandler {
    private final KeyValueStore keyValueStore;
    
    public IncrCommandHandler(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "INCR requires a key");
            return;
        }
        
        String key = args[1];
        
        try {
            long newValue = keyValueStore.increment(key);
            sendInteger(out, (int) newValue);
        } catch (IllegalArgumentException e) {
            sendError(out, e.getMessage());
        }
    }
    
    @Override
    public String getCommandName() {
        return "INCR";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true; // INCR modifies data
    }
}