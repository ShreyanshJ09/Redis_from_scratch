import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class LrangeCommandHandler extends BaseCommandHandler {
    private final ListStore listStore;
    
    public LrangeCommandHandler(ListStore listStore) {
        this.listStore = listStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 4) {
            sendError(out, "LRANGE requires key, start, and stop");
            return;
        }
        
        String key = args[1];
        
        try {
            int start = Integer.parseInt(args[2]);
            int stop = Integer.parseInt(args[3]);
            
            List<String> values = listStore.lrange(key, start, stop);
            sendArray(out, values);
        } catch (NumberFormatException e) {
            sendError(out, "start and stop must be numbers");
        }
    }
    
    @Override
    public String getCommandName() {
        return "LRANGE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // LRANGE only reads data
    }
}