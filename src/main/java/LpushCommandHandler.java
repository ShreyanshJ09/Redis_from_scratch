import java.io.IOException;
import java.io.OutputStream;

public class LpushCommandHandler extends BaseCommandHandler {
    private final ListStore listStore;
    
    public LpushCommandHandler(ListStore listStore) {
        this.listStore = listStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 3) {
            sendError(out, "LPUSH requires key and value");
            return;
        }
        
        String key = args[1];
        String value = args[2];
        
        WakeUpResult result = listStore.lpush(key, value);
        
        if (result != null) {
            // Someone was blocked waiting for this value
            try {
                sendBulkString(result.client.out, value);
            } catch (IOException ignored) {}
            sendInteger(out, 1);
        } else {
            // Normal push
            int length = listStore.llen(key);
            sendInteger(out, length);
        }
    }
    
    @Override
    public String getCommandName() {
        return "LPUSH";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true; // LPUSH modifies data
    }
}