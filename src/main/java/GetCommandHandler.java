import java.io.IOException;
import java.io.OutputStream;

public class GetCommandHandler extends BaseCommandHandler {
    private final KeyValueStore keyValueStore;
    
    public GetCommandHandler(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "GET requires a key");
            return;
        }
        
        String key = args[1];
        
        if (keyValueStore.exists(key)) {
            sendBulkString(out, keyValueStore.get(key));
        } else {
            sendNullBulkString(out);
        }
    }
    
    @Override
    public String getCommandName() {
        return "GET";
    }
}