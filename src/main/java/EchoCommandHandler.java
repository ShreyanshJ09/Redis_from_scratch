import java.io.IOException;
import java.io.OutputStream;

public class EchoCommandHandler extends BaseCommandHandler {
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "ECHO requires an argument");
            return;
        }
        
        sendBulkString(out, args[1]);
    }
    
    @Override
    public String getCommandName() {
        return "ECHO";
    }
}