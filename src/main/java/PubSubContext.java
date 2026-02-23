import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;


public class PubSubContext {
    private final Set<String> subscribedChannels = new HashSet<>();
    private final PubSubManager pubSubManager;
    private final OutputStream clientOut;
    
    public PubSubContext(PubSubManager pubSubManager, OutputStream clientOut) {
        this.pubSubManager = pubSubManager;
        this.clientOut = clientOut;
    }
    
    public boolean isSubscribed() {
        return !subscribedChannels.isEmpty();
    }
    
    public boolean subscribe(String channel) {
        boolean isNew = subscribedChannels.add(channel);
        
        if (pubSubManager != null) {
            pubSubManager.subscribe(channel, clientOut);
        }
        
        return isNew;
    }
    
    public boolean unsubscribe(String channel) {
        boolean removed = subscribedChannels.remove(channel);
        
        // Unregister from global manager
        if (removed && pubSubManager != null) {
            pubSubManager.unsubscribe(channel, clientOut);
        }
        
        return removed;
    }
    
    public int getSubscriptionCount() {
        return subscribedChannels.size();
    }
    
    public Set<String> getSubscribedChannels() {
        return new HashSet<>(subscribedChannels);
    }
    
    public boolean isSubscribedTo(String channel) {
        return subscribedChannels.contains(channel);
    }
    
    public void clearSubscriptions() {
        if (pubSubManager != null) {
            pubSubManager.unsubscribeAll(clientOut);
        }
        
        subscribedChannels.clear();
    }
}