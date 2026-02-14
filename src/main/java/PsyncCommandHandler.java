import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PsyncCommandHandler extends BaseCommandHandler {
    private final String masterReplId;
    private final byte[] emptyRdbFile;
    private final List<OutputStream> connectedReplicas;
    
    public PsyncCommandHandler(String masterReplId, byte[] emptyRdbFile,List<OutputStream> connectedReplicas) {
        this.masterReplId = masterReplId;
        this.emptyRdbFile = emptyRdbFile;
        this.connectedReplicas = connectedReplicas;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        // Step 1: Send FULLRESYNC response
        String response = "FULLRESYNC " + masterReplId + " 0";
        sendSimpleString(out, response);
        
        // Step 2: Send empty RDB file
        // Format: $<length>\r\n<contents>
        // Note: No trailing \r\n after contents (not a standard bulk string)
        String rdbHeader = "$" + emptyRdbFile.length + "\r\n";
        out.write(rdbHeader.getBytes(StandardCharsets.UTF_8));
        out.write(emptyRdbFile);
        out.flush();
        
        // Step 3: Register this connection as a replica for command propagation
        synchronized (connectedReplicas) {
            connectedReplicas.add(out);
        }
        System.out.println("Replica registered for command propagation");
    }
    
    @Override
    public String getCommandName() {
        return "PSYNC";
    }
}