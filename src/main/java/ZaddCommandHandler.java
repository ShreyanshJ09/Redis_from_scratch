import java.io.IOException;
import java.io.OutputStream;

public class ZaddCommandHandler extends BaseCommandHandler {
    private final SortedSetStore sortedSetStore;
    
    public ZaddCommandHandler(SortedSetStore sortedSetStore) {
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // ZADD key score member [score member ...]
        if (args.length < 4 || (args.length - 1) % 2 != 1) {
            sendError(out, "wrong number of arguments for 'zadd' command");
            return;
        }
        
        String key = args[1];
        int newMembersAdded = 0;
        
        // Process score-member pairs
        for (int i = 2; i < args.length; i += 2) {
            try {
                double score = Double.parseDouble(args[i]);
                String member = args[i + 1];
                
                int added = sortedSetStore.zadd(key, score, member);
                newMembersAdded += added;
            } catch (NumberFormatException e) {
                sendError(out, "value is not a valid float");
                return;
            }
        }
        
        sendInteger(out, newMembersAdded);
    }
    
    @Override
    public String getCommandName() {
        return "ZADD";
    }
    
    @Override
    public boolean isWriteCommand() {
        return true;
    }
}