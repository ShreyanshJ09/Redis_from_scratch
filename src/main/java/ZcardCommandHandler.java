import java.io.IOException;
import java.io.OutputStream;


public class ZcardCommandHandler extends BaseCommandHandler {
    private final SortedSetStore sortedSetStore;
    
    public ZcardCommandHandler(SortedSetStore sortedSetStore) {
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // ZCARD key
        if (args.length != 2) {
            sendError(out, "wrong number of arguments for 'zcard' command");
            return;
        }
        
        String key = args[1];
        int cardinality = sortedSetStore.zcard(key);
        
        sendInteger(out, cardinality);
    }
    
    @Override
    public String getCommandName() {
        return "ZCARD";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false;
    }
}