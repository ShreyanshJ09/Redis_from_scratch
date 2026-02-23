import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class PubSubManager {
    private final Map<String, Set<OutputStream>> channelSubscribers = new ConcurrentHashMap<>();
    
    private final Map<OutputStream, Set<String>> clientChannels = new ConcurrentHashMap<>();
    

    public synchronized boolean subscribe(String channel, OutputStream clientOut) {
        channelSubscribers.computeIfAbsent(channel, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                          .add(clientOut);
        
        clientChannels.computeIfAbsent(clientOut, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                      .add(channel);
        
        return true;
    }
    

    public synchronized boolean unsubscribe(String channel, OutputStream clientOut) {
        Set<OutputStream> subscribers = channelSubscribers.get(channel);
        if (subscribers != null) {
            subscribers.remove(clientOut);
            if (subscribers.isEmpty()) {
                channelSubscribers.remove(channel);
            }
        }
        
        Set<String> channels = clientChannels.get(clientOut);
        if (channels != null) {
            boolean removed = channels.remove(channel);
            if (channels.isEmpty()) {
                clientChannels.remove(clientOut);
            }
            return removed;
        }
        
        return false;
    }
    
    public int getSubscriberCount(String channel) {
        Set<OutputStream> subscribers = channelSubscribers.get(channel);
        return subscribers != null ? subscribers.size() : 0;
    }
    

    public Set<OutputStream> getSubscribers(String channel) {
        Set<OutputStream> subscribers = channelSubscribers.get(channel);
        return subscribers != null ? new HashSet<>(subscribers) : Collections.emptySet();
    }
    
    public synchronized void unsubscribeAll(OutputStream clientOut) {
        Set<String> channels = clientChannels.remove(clientOut);
        if (channels != null) {
            for (String channel : channels) {
                Set<OutputStream> subscribers = channelSubscribers.get(channel);
                if (subscribers != null) {
                    subscribers.remove(clientOut);
                    if (subscribers.isEmpty()) {
                        channelSubscribers.remove(channel);
                    }
                }
            }
        }
    }
    
   
    public Set<String> getClientChannels(OutputStream clientOut) {
        Set<String> channels = clientChannels.get(clientOut);
        return channels != null ? new HashSet<>(channels) : Collections.emptySet();
    }
}