import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
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

public class Client {
	
	static String myPath = "/jobs";
    static ZkConnector zkc;
    static Watcher watcher;
    static boolean reconnect=false;
    
    static Socket socket = null;
    static ObjectOutputStream  out = null;
    static ObjectInputStream in = null;
    
    Client(String hosts){
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
    
    public static void main(String[] args){
    	if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Client zkServer:Port");
            return;
        }
        
        String hosts = args[0];
        Client t = new Client(hosts);
        System.out.println("request <password> to create new task");
        System.out.println("query <password> to query the status of created tasks");
        while (true){
        	workPacket output=new workPacket();
		   	workPacket reply=new workPacket();
        	   try{
        		   
        		   	String input; 		   	  		   		
	               	Stat stat = zkc.exists(myPath, watcher); 
	               	//System.out.println("data length is "+stat.getDataLength());
	               	byte[] nodeInfo=zkc.getData(myPath, stat);
	               	String conInfo=new String(nodeInfo);
	               	System.out.println("con info is "+conInfo);
	               	String[] host_Port=conInfo.split("_");
	               	socket=new Socket(host_Port[0],Integer.parseInt(host_Port[1]));
	               	in=new ObjectInputStream(socket.getInputStream());
	               	out=new ObjectOutputStream(socket.getOutputStream());
	               	
	               	if (reconnect=true)
      		   		{
      		   			out.writeObject(output);
	      		   		reply=(workPacket) in.readObject();
	               		
	               		if(reply.type==workPacket.jobQueryReply||reply.type==workPacket.jobReply){
	               			System.out.println(reply.ReplyMsg);
	               		}
	               		reconnect=false;
      		   			continue;
      		   		}else{
		               	BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		               	System.out.print("next command: ");
		               	if((input = stdIn.readLine()) != null){
		               		String[] inputSplit=input.split(" ");
		               		
		               		if(inputSplit[0].toLowerCase().equals("request")){
		               			output.type=workPacket.jobRequest;
		               			output.hashedPassword=inputSplit[1];	
		               		}else if(inputSplit[0].toLowerCase().equals("query")){
		               			output.type=workPacket.jobQuery;
		               			output.hashedPassword=inputSplit[1];
		               		}
		               		out.writeObject(output);
		               		reply=(workPacket) in.readObject();
		               		
		               		if(reply.type==workPacket.jobQueryReply||reply.type==workPacket.jobReply){
		               			System.out.println(reply.ReplyMsg);
		               		}
		               	}
	               	}
	               		
	               	in.close();
	                out.close();
	                socket.close();
               }
               catch(Exception e){
            	   System.out.println("somethinh happened gotta reconnect");
            	   reconnect=true;
            	   try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	               
               }
        	
        }
     
        
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
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
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
        if(path.equalsIgnoreCase(myPath)) {
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
