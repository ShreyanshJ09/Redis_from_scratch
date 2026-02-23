/**
 * Stores RDB file configuration parameters.
 * 
 * These parameters specify where the RDB file is located:
 * - dir: The directory path (e.g., "/tmp/redis-files")
 * - dbfilename: The RDB filename (e.g., "dump.rdb")
 */
public class RDBConfig {
    private String dir;
    private String dbfilename;
    
    public RDBConfig() {
        // Default values
        this.dir = "/tmp/redis-files";
        this.dbfilename = "dump.rdb";
    }
    
    public RDBConfig(String dir, String dbfilename) {
        this.dir = dir;
        this.dbfilename = dbfilename;
    }
    
    public String getDir() {
        return dir;
    }
    
    public void setDir(String dir) {
        this.dir = dir;
    }
    
    public String getDbfilename() {
        return dbfilename;
    }
    
    public void setDbfilename(String dbfilename) {
        this.dbfilename = dbfilename;
    }
    
    public String getFullPath() {
        return dir + "/" + dbfilename;
    }
}