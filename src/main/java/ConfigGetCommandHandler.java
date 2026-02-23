import java.io.IOException;
import java.io.OutputStream;


public class ConfigGetCommandHandler extends BaseCommandHandler {
    private final RDBConfig rdbConfig;
    
    public ConfigGetCommandHandler(RDBConfig rdbConfig) {
        this.rdbConfig = rdbConfig;
    }
    
    @Override
    public void execute(String[] args, OutputStream out) throws IOException {
        if (args.length < 3) {
            sendError(out, "ERR wrong number of arguments for 'config|get' command");
            return;
        }
        
        String subcommand = args[1].toUpperCase();
        if (!subcommand.equals("GET")) {
            sendError(out, "ERR Unknown CONFIG subcommand '" + args[1] + "'");
            return;
        }
        
        String parameter = args[2];
        String value = getConfigValue(parameter);
        
        if (value == null) {
            sendEmptyArray(out);
            return;
        }
        
        sendConfigResponse(out, parameter, value);
    }
    
    private String getConfigValue(String parameter) {
        switch (parameter.toLowerCase()) {
            case "dir":
                return rdbConfig.getDir();
            case "dbfilename":
                return rdbConfig.getDbfilename();
            default:
                return null;
        }
    }
    

    private void sendConfigResponse(OutputStream out, String parameter, String value) throws IOException {
        StringBuilder response = new StringBuilder();
        
        response.append("*2\r\n");
        
        response.append("$").append(parameter.length()).append("\r\n");
        response.append(parameter).append("\r\n");
        
        response.append("$").append(value.length()).append("\r\n");
        response.append(value).append("\r\n");
        
        out.write(response.toString().getBytes());
        out.flush();
    }
    
    private void sendEmptyArray(OutputStream out) throws IOException {
        out.write("*0\r\n".getBytes());
        out.flush();
    }
    
    @Override
    public String getCommandName() {
        return "CONFIG";
    }
}