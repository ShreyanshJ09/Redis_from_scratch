import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;


public class CommandRegistry {
    private final Map<String, CommandHandler> handlers = new HashMap<>();
    

    public void register(CommandHandler handler) {
        handlers.put(handler.getCommandName().toUpperCase(), handler);
    }
    

    public boolean executeCommand(String command, String[] args, OutputStream out) throws IOException {
        CommandHandler handler = handlers.get(command.toUpperCase());
        
        if (handler == null) {
            return false;
        }
        
        handler.execute(args, out);
        return true;
    }
    

    public boolean isWriteCommand(String command) {
        CommandHandler handler = handlers.get(command.toUpperCase());
        return handler != null && handler.isWriteCommand();
    }
    

    public CommandHandler getHandler(String command) {
        return handlers.get(command.toUpperCase());
    }
}