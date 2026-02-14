import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;


public abstract class BaseCommandHandler implements CommandHandler {
    
    protected void sendSimpleString(OutputStream out, String msg) throws IOException {
        out.write(("+" + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    protected void sendBulkString(OutputStream out, String msg) throws IOException {
        out.write(("$" + msg.length() + "\r\n" + msg + "\r\n")
                .getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    protected void sendNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    protected void sendError(OutputStream out, String msg) throws IOException {
        out.write(("-ERR " + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    protected void sendInteger(OutputStream out, int value) throws IOException {
        out.write((":" + value + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
    
    protected void sendArray(OutputStream out, List<String> items) throws IOException {
        out.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        
        for (String item : items) {
            out.write(("$" + item.length() + "\r\n" + item + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        out.flush();
    }
}