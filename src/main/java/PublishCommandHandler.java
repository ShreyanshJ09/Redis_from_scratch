import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class PublishCommandHandler extends BaseCommandHandler {
    private final PubSubManager pubSubManager;
    
    public PublishCommandHandler(PubSubManager pubSubManager) {
        this.pubSubManager = pubSubManager;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 3) {
            sendError(out, "PUBLISH requires channel and message");
            return;
        }
        
        String channel = args[1];
        String message = args[2];
        
        Set<OutputStream> subscribers = pubSubManager.getSubscribers(channel);
        int subscriberCount = subscribers.size();
        
        deliverMessageToSubscribers(channel, message, subscribers);
        
        sendInteger(out, subscriberCount);
    }

    private void deliverMessageToSubscribers(String channel, String message, Set<OutputStream> subscribers) {
        if (subscribers.isEmpty()) {
            return;
        }
        
        StringBuilder messageArray = new StringBuilder();
        
        messageArray.append("*3\r\n");
        
        messageArray.append("$7\r\nmessage\r\n");
        
        messageArray.append("$").append(channel.length()).append("\r\n");
        messageArray.append(channel).append("\r\n");
        
        messageArray.append("$").append(message.length()).append("\r\n");
        messageArray.append(message).append("\r\n");
        
        byte[] messageBytes = messageArray.toString().getBytes(StandardCharsets.UTF_8);
        
        for (OutputStream subscriber : subscribers) {
            try {
                subscriber.write(messageBytes);
                subscriber.flush();
                System.out.println("Delivered message to subscriber on channel: " + channel);
            } catch (IOException e) {
                System.err.println("Failed to deliver message to subscriber: " + e.getMessage());
            }
        }
    }
    
    @Override
    public String getCommandName() {
        return "PUBLISH";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false;
    }
}