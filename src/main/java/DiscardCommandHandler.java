import java.io.IOException;
import java.io.OutputStream;

public class DiscardCommandHandler extends BaseCommandHandler {
    private final TransactionContext transactionContext;
    
    public DiscardCommandHandler(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (!transactionContext.isInTransaction()) {
            sendError(out, "DISCARD without MULTI");
            return;
        }
        
        transactionContext.endTransaction();
        sendSimpleString(out, "OK");
    }
    
    @Override
    public String getCommandName() {
        return "DISCARD";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // DISCARD doesn't modify data
    }
}