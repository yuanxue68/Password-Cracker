import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Socket;

public class Worker {
	String fileServerPath="/fileServer";
	String jobPath="/jobs";
	String workDone="/workDone";
	String passSolved="/passSolved";
	
	public ZkConnector zkc;
	Watcher watcher;
	static String zkhost;
	static String host;
	static int port;
	
	static Socket socket = null;
    static ObjectOutputStream  out = null;
    static ObjectInputStream in = null;
    
    public Worker(String zkhost2) {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args){
    	if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:Port");
            return;
        }
        
        String zkhost = args[0];
        
        Worker worker=new Worker(zkhost);
    }

	public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }
	
	private void checkpath() {
        Stat stat = zkc.exists(fileServerPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + fileServerPath);
            Code ret = zkc.create(
            			fileServerPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("created jobs in client wut?");
        } else{
        	System.out.println("null /jobs huh?");
        }
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(fileServerPath)) {
            if (type == EventType.NodeDeleted) {
                //System.out.println(myPath + " deleted! Let's go!");       
                //checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                //System.out.println(myPath + " created!");       
                //try{ Thread.sleep(5000); } catch (Exception e) {}
                //checkpath(); // re-enable the watch
            }
        }
    }
}
