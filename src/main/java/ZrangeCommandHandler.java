import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ZrangeCommandHandler extends BaseCommandHandler {
    private final SortedSetStore sortedSetStore;
    
    public ZrangeCommandHandler(SortedSetStore sortedSetStore) {
        this.sortedSetStore = sortedSetStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // ZRANGE key start stop
        if (args.length != 4) {
            sendError(out, "wrong number of arguments for 'zrange' command");
            return;
        }
        
        String key = args[1];
        
        try {
            int start = Integer.parseInt(args[2]);
            int stop = Integer.parseInt(args[3]);
            
            List<String> members = sortedSetStore.zrange(key, start, stop);
            
            sendArray(out, members);
        } catch (NumberFormatException e) {
            sendError(out, "value is not an integer or out of range");
        }
    }
    
    @Override
    public String getCommandName() {
        return "ZRANGE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false;
    }
}