import java.io.IOException;
import java.io.OutputStream;

public class SetCommandHandler extends BaseCommandHandler {
    private final KeyValueStore keyValueStore;
    
    public SetCommandHandler(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 3) {
            sendError(out, "SET requires key and value");
            return;
        }
        
        String key = args[1];
        String value = args[2];
        long expiryMillis = Long.MAX_VALUE;
        
        // Parse optional expiry arguments
        if (args.length >= 5) {
            String optionName = args[3].toUpperCase();
            try {
                int timeValue = Integer.parseInt(args[4]);
                if ("EX".equals(optionName)) {
                    expiryMillis = timeValue * 1000L;
                } else if ("PX".equals(optionName)) {
                    expiryMillis = timeValue;
                }
            } catch (NumberFormatException ignored) {}
        }
        
        keyValueStore.set(key, value, expiryMillis);
        sendSimpleString(out, "OK");
    }
    
    @Override
    public String getCommandName() {
        return "SET";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true;
    }
}