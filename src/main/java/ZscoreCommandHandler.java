import java.io.IOException;
import java.io.OutputStream;

/**
 * Handles the ZSCORE command.
 * 
 * Format: ZSCORE key member
 * 
 * Returns the score of member in the sorted set at key.
 * Returns null bulk string if the member or key does not exist.
 */
public class ZscoreCommandHandler extends BaseCommandHandler {
    private final SortedSetStore sortedSetStore;
    
    public ZscoreCommandHandler(SortedSetStore sortedSetStore) {
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // ZSCORE key member
        if (args.length != 3) {
            sendError(out, "wrong number of arguments for 'zscore' command");
            return;
        }
        
        String key = args[1];
        String member = args[2];
        
        Double score = sortedSetStore.zscore(key, member);
        
        if (score == null) {
            // Member or key doesn't exist
            sendNullBulkString(out);
        } else {
            // Send score as bulk string
            sendBulkString(out, String.valueOf(score));
        }
    }
    
    @Override
    public String getCommandName() {
        return "ZSCORE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false;
    }
}