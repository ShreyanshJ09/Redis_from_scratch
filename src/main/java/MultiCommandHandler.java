import java.io.IOException;
import java.io.OutputStream;

public class MultiCommandHandler extends BaseCommandHandler {
    private final TransactionContext transactionContext;
    
    public MultiCommandHandler(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        transactionContext.startTransaction();
        sendSimpleString(out, "OK");
    }
    
    @Override
    public String getCommandName() {
        return "MULTI";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // MULTI itself doesn't modify data
    }
}