import java.io.IOException;
import java.io.OutputStream;

public class TypeCommandHandler extends BaseCommandHandler {
    private final KeyValueStore keyValueStore;
    private final ListStore listStore;
    private final StreamStore streamStore;
    private final SortedSetStore sortedSetStore;
    
    public TypeCommandHandler(KeyValueStore keyValueStore, ListStore listStore, StreamStore streamStore, SortedSetStore sortedSetStore) {
        this.keyValueStore = keyValueStore;
        this.listStore = listStore;
        this.streamStore = streamStore;
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "TYPE requires a key");
            return;
        }
        
        String key = args[1];
        
        // Check which store contains the key (order matters - most specific first)
        if (sortedSetStore.exists(key)) {
            sendSimpleString(out, "zset");
        } else if (streamStore.exists(key)) {
            sendSimpleString(out, "stream");
        } else if (listStore.exists(key)) {
            sendSimpleString(out, "list");
        } else if (keyValueStore.exists(key)) {
            sendSimpleString(out, "string");
        } else {
            sendSimpleString(out, "none");
        }
    }
    
    @Override
    public String getCommandName() {
        return "TYPE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // TYPE only reads data
    }
}