import java.io.IOException;
import java.io.OutputStream;

public class InfoCommandHandler extends BaseCommandHandler {
    private final String serverRole;
    private final String masterReplId;
    private final int masterReplOffset;
    
    public InfoCommandHandler(String serverRole, String masterReplId, int masterReplOffset) {
        this.serverRole = serverRole;
        this.masterReplId = masterReplId;
        this.masterReplOffset = masterReplOffset;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        String section = args.length >= 2 ? args[1].toLowerCase() : "all";
        
        if (section.equals("replication") || section.equals("all")) {
            StringBuilder info = new StringBuilder();
            info.append("# Replication\r\n");
            info.append("role:").append(serverRole).append("\r\n");
            info.append("master_replid:").append(masterReplId).append("\r\n");
            info.append("master_repl_offset:").append(masterReplOffset).append("\r\n");
            sendBulkString(out, info.toString());
        } else {
            sendBulkString(out, "");
        }
    }
    
    @Override
    public String getCommandName() {
        return "INFO";
    }
}