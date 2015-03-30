import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

public class JobTracker {
	 String myPath = "/jobs";
	 String workingPath="/working";
	 String workDone="/workDone";
	 String passSolved="/passSolved";
	 
	 public ZkConnector zkc;
	 Watcher watcher;
	 static String host;
	 static int port;
	 
	 public JobTracker(String hosts) {
		 zkc = new ZkConnector();
	        try {
	            zkc.connect(hosts);
	        } catch(Exception e) {
	            System.out.println("Zookeeper connect "+ e.getMessage());
	        }
	 
	        watcher = new Watcher() { // Anonymous Watcher
	                            @Override
	                            public void process(WatchedEvent event) {
	                                handleEvent(event);
	                        
	                            } };
	}

	public static void main(String[] args) {
	      
	        if (args.length != 2) {
	            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
	            return;
	        }

	        try{
	        	host=InetAddress.getLocalHost().getHostName();
	        	port=Integer.parseInt(args[1]);
		        
		        JobTracker t = new JobTracker(args[0]);
		        
		        t.checkpath();
		        
		        ServerSocket serverSocket = new ServerSocket(port);
	            while (true) {
	                Socket socket = serverSocket.accept();
	                new JobTrackerThread(host,socket).start();
	            }
	        }
	        catch(Exception e){
	        	
	        }
	       
	 }
	
	private void checkpath() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("the boss jobtracker");
        } 
        
        Stat workingStat = zkc.exists(workingPath, watcher);
        if (workingStat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + workingPath);
            Code ret = zkc.create(
            			workingPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("created working path");
        } 
        
        Stat workDoneStat = zkc.exists(workDone, watcher);
        if (workDoneStat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + workDone);
            Code ret = zkc.create(
                        workDone,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("created workDone path");
        } 
        
        Stat solvedStat = zkc.exists(passSolved, watcher);
        if (solvedStat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + passSolved);
            Code ret = zkc.create(
                        passSolved,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("created passSolved path");
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

}
