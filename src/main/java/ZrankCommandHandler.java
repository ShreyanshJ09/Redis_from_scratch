import java.io.IOException;
import java.io.OutputStream;

public class ZrankCommandHandler extends BaseCommandHandler {
    private final SortedSetStore sortedSetStore;
    
    public ZrankCommandHandler(SortedSetStore sortedSetStore) {
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // ZRANK key member
        if (args.length != 3) {
            sendError(out, "wrong number of arguments for 'zrank' command");
            return;
        }
        
        String key = args[1];
        String member = args[2];
        
        Integer rank = sortedSetStore.zrank(key, member);
        
        if (rank == null) {
            // Member or key doesn't exist
            sendNullBulkString(out);
        } else {
            sendInteger(out, rank);
        }
    }
    
    @Override
    public String getCommandName() {
        return "ZRANK";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false;
    }
}