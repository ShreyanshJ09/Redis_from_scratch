import java.io.IOException;
import java.io.OutputStream;

public class ReplconfCommandHandler extends BaseCommandHandler {
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // For now, we just acknowledge REPLCONF commands with OK
        // The actual arguments (listening-port, capa) can be ignored
        sendSimpleString(out, "OK");
    }
    
    @Override
    public String getCommandName() {
        return "REPLCONF";
    }
}