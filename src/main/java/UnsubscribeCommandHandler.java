import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;


public class UnsubscribeCommandHandler extends BaseCommandHandler {
    private final PubSubContext pubSubContext;
    
    public UnsubscribeCommandHandler(PubSubContext pubSubContext) {
        this.pubSubContext = pubSubContext;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) {
            unsubscribeFromAllChannels(out);
            return;
        }
        
        for (int i = 1; i < args.length; i++) {
            String channel = args[i];
            
            pubSubContext.unsubscribe(channel);
            
            sendUnsubscribeResponse(out, channel, pubSubContext.getSubscriptionCount());
        }
    }
    

    private void unsubscribeFromAllChannels(OutputStream out) throws IOException {
        java.util.Set<String> channels = pubSubContext.getSubscribedChannels();
        
        if (channels.isEmpty()) {
            sendUnsubscribeResponse(out, null, 0);
            return;
        }
        
        for (String channel : new java.util.ArrayList<>(channels)) {
            pubSubContext.unsubscribe(channel);
            sendUnsubscribeResponse(out, channel, pubSubContext.getSubscriptionCount());
        }
    }
    

    private void sendUnsubscribeResponse(OutputStream out, String channel, int remainingCount) throws IOException {
        StringBuilder response = new StringBuilder();
        
        response.append("*3\r\n");
        
        response.append("$11\r\nunsubscribe\r\n");
        
        if (channel != null) {
            response.append("$").append(channel.length()).append("\r\n");
            response.append(channel).append("\r\n");
        } else {
            response.append("$-1\r\n"); 
        }
        
        response.append(":").append(remainingCount).append("\r\n");
        
        out.write(response.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    @Override
    public String getCommandName() {
        return "UNSUBSCRIBE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false;
    }
}