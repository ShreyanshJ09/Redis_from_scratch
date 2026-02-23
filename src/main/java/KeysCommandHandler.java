import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class KeysCommandHandler extends BaseCommandHandler {
    private final KeyValueStore keyValueStore;
    private final ListStore listStore;
    private final StreamStore streamStore;
    
    public KeysCommandHandler(KeyValueStore keyValueStore, ListStore listStore, StreamStore streamStore) {
        this.keyValueStore = keyValueStore;
        this.listStore = listStore;
        this.streamStore = streamStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "ERR wrong number of arguments for 'keys' command");
            return;
        }
        
        String pattern = args[1];
        
        if (!pattern.equals("*")) {
            sendError(out, "ERR pattern matching not implemented, use '*'");
            return;
        }
        
        List<String> allKeys = new ArrayList<>();
        
        allKeys.addAll(keyValueStore.getAllKeys());
        
        allKeys.addAll(listStore.getAllKeys());
        
        allKeys.addAll(streamStore.getAllKeys());
        
        sendArray(out, allKeys);
    }
    
    @Override
    public String getCommandName() {
        return "KEYS";
    }
}