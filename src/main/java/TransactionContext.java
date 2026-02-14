import java.util.ArrayList;
import java.util.List;

/**
 * Stores the state of a transaction for a client connection
 */
public class TransactionContext {
    private boolean inTransaction = false;
    private final List<QueuedCommand> commandQueue = new ArrayList<>();
    
    public boolean isInTransaction() {
        return inTransaction;
    }
    
    public void startTransaction() {
        inTransaction = true;
        commandQueue.clear();
    }
    
    public void queueCommand(String command, String[] args) {
        commandQueue.add(new QueuedCommand(command, args));
    }
    
    public List<QueuedCommand> getCommandQueue() {
        return new ArrayList<>(commandQueue);  // Return copy
    }
    
    public void endTransaction() {
        inTransaction = false;
        commandQueue.clear();
    }
    
    public void clearQueue() {
        commandQueue.clear();
    }
}


class QueuedCommand {
    final String command;
    final String[] args;
    
    QueuedCommand(String command, String[] args) {
        this.command = command;
        this.args = args.clone();  // Defensive copy
    }
}