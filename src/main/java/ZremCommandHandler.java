import java.io.IOException;
import java.io.OutputStream;


public class ZremCommandHandler extends BaseCommandHandler {
    private final SortedSetStore sortedSetStore;
    
    public ZremCommandHandler(SortedSetStore sortedSetStore) {
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // ZREM key member [member ...]
        if (args.length < 3) {
            sendError(out, "wrong number of arguments for 'zrem' command");
            return;
        }
        
        String key = args[1];
        int removedCount = 0;
        
        // Remove each specified member
        for (int i = 2; i < args.length; i++) {
            String member = args[i];
            removedCount += sortedSetStore.zrem(key, member);
        }
        
        sendInteger(out, removedCount);
    }
    
    @Override
    public String getCommandName() {
        return "ZREM";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true;  // ZREM modifies data
    }
}