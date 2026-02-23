import java.util.HashSet;
import java.util.Set;

public class PubSubContext {
    private final Set<String> subscribedChannels = new HashSet<>();
    

    public boolean isSubscribed() {
        return !subscribedChannels.isEmpty();
    }
    

    public boolean subscribe(String channel) {
        return subscribedChannels.add(channel);
    }
    

    public boolean unsubscribe(String channel) {
        return subscribedChannels.remove(channel);
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
        subscribedChannels.clear();
    }
}