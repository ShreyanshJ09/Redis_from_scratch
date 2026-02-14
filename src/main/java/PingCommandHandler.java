import java.io.IOException;
import java.io.OutputStream;

public class PingCommandHandler extends BaseCommandHandler {
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        sendSimpleString(out, "PONG");
    }
    
    @Override
    public String getCommandName() {
        return "PING";
    }
}