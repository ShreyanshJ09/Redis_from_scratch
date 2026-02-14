import java.io.IOException;
import java.io.OutputStream;

public class LlenCommandHandler extends BaseCommandHandler {
    private final ListStore listStore;
    
    public LlenCommandHandler(ListStore listStore) {
        this.listStore = listStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "LLEN requires a key");
            return;
        }
        
        String key = args[1];
        int length = listStore.llen(key);
        sendInteger(out, length);
    }
    
    @Override
    public String getCommandName() {
        return "LLEN";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // LLEN only reads data
    }
}