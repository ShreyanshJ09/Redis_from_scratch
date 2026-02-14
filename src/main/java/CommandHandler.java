import java.io.IOException;
import java.io.OutputStream;


public interface CommandHandler {
    void execute(String[] args, OutputStream out) throws IOException;
    
    String getCommandName();
    
    default boolean isWriteCommand() {
        return false;
    }
}