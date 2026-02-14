import java.io.IOException;
import java.io.OutputStream;

public class ExistsCommandHandler extends BaseCommandHandler {
    private final KeyValueStore keyValueStore;
    private final ListStore listStore;
    private final StreamStore streamStore;
    
    public ExistsCommandHandler(KeyValueStore keyValueStore, ListStore listStore, StreamStore streamStore) {
        this.keyValueStore = keyValueStore;
        this.listStore = listStore;
        this.streamStore = streamStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "EXISTS requires a key");
            return;
        }
        
        String key = args[1];
        
        // Check if key exists in any store
        boolean exists = keyValueStore.exists(key) || 
                        listStore.exists(key) || 
                        streamStore.exists(key);
        
        sendInteger(out, exists ? 1 : 0);
    }
    
    @Override
    public String getCommandName() {
        return "EXISTS";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // EXISTS only reads data
    }
}