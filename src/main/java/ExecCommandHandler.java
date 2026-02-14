import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ExecCommandHandler extends BaseCommandHandler {
    private final TransactionContext transactionContext;
    private final CommandRegistry commandRegistry;
    
    public ExecCommandHandler(TransactionContext transactionContext, CommandRegistry commandRegistry) {
        this.transactionContext = transactionContext;
        this.commandRegistry = commandRegistry;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (!transactionContext.isInTransaction()) {
            sendError(out, "EXEC without MULTI");
            return;
        }
        
        List<QueuedCommand> commands = transactionContext.getCommandQueue();
        
        if (commands.isEmpty()) {
            out.write("*0\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            transactionContext.endTransaction();
            return;
        }
        
        List<ByteArrayOutputStream> responses = new ArrayList<>();
        
        // Execute each queued command
        for (QueuedCommand cmd : commands) {
            ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
            
            try {
                commandRegistry.executeCommand(cmd.command, cmd.args, responseBuffer);
                responses.add(responseBuffer);
            } catch (Exception e) {
                ByteArrayOutputStream errorBuffer = new ByteArrayOutputStream();
                String errorMsg = e.getMessage() != null ? e.getMessage() : "command failed";
                errorBuffer.write(("-ERR " + errorMsg + "\r\n").getBytes(StandardCharsets.UTF_8));
                responses.add(errorBuffer);
            }
        }
        
        // Send array of responses
        out.write(("*" + responses.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (ByteArrayOutputStream response : responses) {
            out.write(response.toByteArray());
        }
        out.flush();
        
        transactionContext.endTransaction();
    }
    
    @Override
    public String getCommandName() {
        return "EXEC";
    }
    
    @Override
    public boolean isWriteCommand() {
        // EXEC can contain write commands, but we handle propagation differently
        // The individual commands in the transaction will be propagated
        return false;
    }
}