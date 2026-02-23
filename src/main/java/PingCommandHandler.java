import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class PingCommandHandler extends BaseCommandHandler {
    private final PubSubContext pubSubContext;
    
    public PingCommandHandler(PubSubContext pubSubContext) {
        this.pubSubContext = pubSubContext;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // Check if client is in subscribed mode
        if (pubSubContext != null && pubSubContext.isSubscribed()) {
            // In subscribed mode: respond with ["pong", ""]
            sendPingSubscribedResponse(out);
        } else {
            // Normal mode: respond with +PONG
            sendSimpleString(out, "PONG");
        }
    }

    private void sendPingSubscribedResponse(OutputStream out) throws IOException {
        StringBuilder response = new StringBuilder();
        
        response.append("*2\r\n");
        
        response.append("$4\r\npong\r\n");
        
        response.append("$0\r\n\r\n");
        
        out.write(response.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    @Override
    public String getCommandName() {
        return "PING";
    }
}