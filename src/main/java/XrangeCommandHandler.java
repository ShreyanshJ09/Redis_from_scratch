import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class XrangeCommandHandler extends BaseCommandHandler {
    private final StreamStore streamStore;
    
    public XrangeCommandHandler(StreamStore streamStore) {
        this.streamStore = streamStore;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 4) {
            sendError(out, "XRANGE requires key, start, and end");
            return;
        }
        
        String key = args[1];
        String startId = args[2];
        String endId = args[3];
        
        List<StreamEntry> entries = streamStore.xrange(key, startId, endId);
        sendStreamEntries(out, entries);
    }
    
    private void sendStreamEntries(OutputStream out, List<StreamEntry> entries) throws IOException {
        out.write(("*" + entries.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (StreamEntry entry : entries) {
            out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
            
            String id = entry.getId();
            out.write(("$" + id.length() + "\r\n" + id + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            Map<String, String> fields = entry.getFields();
            int fieldCount = fields.size() * 2;
            out.write(("*" + fieldCount + "\r\n").getBytes(StandardCharsets.UTF_8));
            
            for (Map.Entry<String, String> field : fields.entrySet()) {
                String fieldName = field.getKey();
                String fieldValue = field.getValue();
                out.write(("$" + fieldName.length() + "\r\n" + fieldName + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.write(("$" + fieldValue.length() + "\r\n" + fieldValue + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
        out.flush();
    }
    
    @Override
    public String getCommandName() {
        return "XRANGE";
    }
    
    @Override
    public boolean isWriteCommand() {
        return false; // XRANGE only reads data
    }
}