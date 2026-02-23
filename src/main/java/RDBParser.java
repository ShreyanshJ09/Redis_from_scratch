import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;


public class RDBParser {
    
    private static final int OPCODE_EOF = 0xFF;           // End of file
    private static final int OPCODE_SELECTDB = 0xFE;      // Database selector
    private static final int OPCODE_EXPIRETIME = 0xFD;    // Expire time in seconds
    private static final int OPCODE_EXPIRETIMEMS = 0xFC;  // Expire time in milliseconds
    private static final int OPCODE_RESIZEDB = 0xFB;      // Hash table size info
    private static final int OPCODE_AUX = 0xFA;           // Auxiliary metadata
    
    private static final int TYPE_STRING = 0;
    

    public static void loadRDB(String filepath, KeyValueStore keyValueStore) {
        File file = new File(filepath);
        
        if (!file.exists()) {
            System.out.println("RDB file does not exist: " + filepath);
            return;
        }
        
        try (FileInputStream fis = new FileInputStream(file);BufferedInputStream bis = new BufferedInputStream(fis)) {
            
            System.out.println("Loading RDB file: " + filepath);
            
            // Read and verify header
            byte[] header = new byte[9];
            bis.read(header);
            String headerStr = new String(header);
            
            if (!headerStr.startsWith("REDIS")) {
                System.err.println("Invalid RDB file: bad magic string");
                return;
            }
            
            System.out.println("RDB version: " + headerStr.substring(5));
            
            parseRDB(bis, keyValueStore);
            
            System.out.println("RDB file loaded successfully");
            
        } catch (IOException e) {
            System.err.println("Error loading RDB file: " + e.getMessage());
            e.printStackTrace();
        }
    }
    

    private static void parseRDB(InputStream in, KeyValueStore keyValueStore) throws IOException {
        while (true) {
            int opcode = in.read();
            
            if (opcode == -1) {
                break; // End of stream
            }
            
            if (opcode == OPCODE_EOF) {
                in.skip(8);
                break;
            } else if (opcode == OPCODE_SELECTDB) {
                int dbIndex = (int) readLength(in);
                System.out.println("Selecting database: " + dbIndex);
            } else if (opcode == OPCODE_RESIZEDB) {
                long dbSize = readLength(in);
                long expiresSize = readLength(in);
                System.out.println("Database size: " + dbSize + ", expires: " + expiresSize);
            } else if (opcode == OPCODE_AUX) {
                String key = readString(in);
                String value = readString(in);
                System.out.println("Metadata: " + key + " = " + value);
            } else if (opcode == OPCODE_EXPIRETIME || opcode == OPCODE_EXPIRETIMEMS) {
                // Key with expiry
                long expiry = readExpiry(in, opcode);
                int valueType = in.read();
                String key = readString(in);
                String value = readValue(in, valueType);
                
                // Calculate expiry in milliseconds from now
                long now = System.currentTimeMillis();
                long expiryMs = (opcode == OPCODE_EXPIRETIME) ? expiry * 1000 : expiry;
                long ttl = expiryMs - now;
                
                if (ttl > 0) {
                    keyValueStore.set(key, value, ttl);
                    System.out.println("Loaded key with expiry: " + key + " = " + value + " (TTL: " + ttl + "ms)");
                } else {
                    System.out.println("Skipping expired key: " + key);
                }
            } else {
                // Regular key-value pair (opcode is the value type)
                int valueType = opcode;
                String key = readString(in);
                String value = readValue(in, valueType);
                
                // No expiry, use Long.MAX_VALUE
                keyValueStore.set(key, value, Long.MAX_VALUE);
                System.out.println("Loaded key: " + key + " = " + value);
            }
        }
    }
    

    private static long readExpiry(InputStream in, int opcode) throws IOException {
        if (opcode == OPCODE_EXPIRETIME) {
            // 4 bytes, seconds
            byte[] bytes = new byte[4];
            in.read(bytes);
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
        } else {
            // 8 bytes, milliseconds
            byte[] bytes = new byte[8];
            in.read(bytes);
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
    }

    private static long readLength(InputStream in) throws IOException {
        int firstByte = in.read();
        int type = (firstByte & 0xC0) >> 6; // First 2 bits
        
        switch (type) {
            case 0: // 00: 6-bit length
                return firstByte & 0x3F;
                
            case 1: // 01: 14-bit length
                int secondByte = in.read();
                return ((firstByte & 0x3F) << 8) | secondByte;
                
            case 2: // 10: 32-bit length
                byte[] bytes = new byte[4];
                in.read(bytes);
                return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt() & 0xFFFFFFFFL;
                
            case 3: // 11: Special encoding
                return -1; // Signal special encoding
                
            default:
                throw new IOException("Invalid length encoding");
        }
    }
    
    /**
     * Read a string-encoded value.
     */
    private static String readString(InputStream in) throws IOException {
        long length = readLength(in);
        
        if (length == -1) {
            // Special string encoding
            int format = in.read() & 0x3F; // Last 6 bits of first byte
            return readSpecialString(in, format);
        }
        
        // Regular string: read 'length' bytes
        byte[] bytes = new byte[(int) length];
        in.read(bytes);
        return new String(bytes);
    }
    
    /**
     * Read specially encoded strings (integers).
     */
    private static String readSpecialString(InputStream in, int format) throws IOException {
        switch (format) {
            case 0: // 8-bit integer
                return String.valueOf(in.read());
                
            case 1: // 16-bit integer (little-endian)
                byte[] bytes16 = new byte[2];
                in.read(bytes16);
                int val16 = ByteBuffer.wrap(bytes16).order(ByteOrder.LITTLE_ENDIAN).getShort();
                return String.valueOf(val16);
                
            case 2: // 32-bit integer (little-endian)
                byte[] bytes32 = new byte[4];
                in.read(bytes32);
                int val32 = ByteBuffer.wrap(bytes32).order(ByteOrder.LITTLE_ENDIAN).getInt();
                return String.valueOf(val32);
                
            default:
                throw new IOException("Unsupported special string format: " + format);
        }
    }
    
    /**
     * Read a value based on its type.
     */
    private static String readValue(InputStream in, int valueType) throws IOException {
        if (valueType == TYPE_STRING) {
            return readString(in);
        }
        
        // For now, only string type is supported
        throw new IOException("Unsupported value type: " + valueType);
    }
}