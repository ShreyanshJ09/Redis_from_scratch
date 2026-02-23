import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;


public class SubscribeCommandHandler extends BaseCommandHandler {
    private final PubSubContext pubSubContext;
    
    public SubscribeCommandHandler(PubSubContext pubSubContext) {
        this.pubSubContext = pubSubContext;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            sendError(out, "SUBSCRIBE requires at least one channel");
            return;
        }
        
        // Subscribe to each channel specified
        for (int i = 1; i < args.length; i++) {
            String channel = args[i];
            
            // Add to subscribed channels (returns true if new, false if already subscribed)
            pubSubContext.subscribe(channel);
            
            // Send subscription confirmation for this channel
            sendSubscribeResponse(out, channel, pubSubContext.getSubscriptionCount());
        }
    }
    
    private void sendSubscribeResponse(OutputStream out, String channel, int count) throws IOException {
        StringBuilder response = new StringBuilder();
        
        response.append("*3\r\n");
        
        response.append("$9\r\nsubscribe\r\n");
        
        response.append("$").append(channel.length()).append("\r\n");
        response.append(channel).append("\r\n");
        
        response.append(":").append(count).append("\r\n");
        
        out.write(response.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    @Override
    public String getCommandName() {
        return "SUBSCRIBE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; 
    }
}