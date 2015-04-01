import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Socket;

public class Worker {
	static String fileServerPath="/fileServer";
	static String jobPath="/jobs";
	static String workDone="/workDone";
	static String passSolved="/passSolved";
	static boolean reconnect=false;
	
	static ZkConnector zkc;
	static Watcher watcher;
	static String zkhost;
	static String host;
	static int port;
	
	static Socket socket = null;
    static ObjectOutputStream  out = null;
    static ObjectInputStream in = null;
    
    public Worker(String hosts) {
		zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
            System.out.println("JobTracker: zk connect in job tracker parent on host "+hosts);
        } catch(Exception e) {
            System.out.println("JobTracker: Zookeeper connect "+ e.getMessage());
        }
 
        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };
	}

	public static void main(String[] args){
    	if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:Port");
            return;
        }
        
        String zkhost = args[0];
        Worker worker=new Worker(zkhost);
        workPacket output=new workPacket();
        workPacket reply=new workPacket();
        while(true){
        	try{
        		ArrayList<String> jobList=new ArrayList<String>();
        		Stat jobStat=zkc.exists(jobPath,watcher);
        		if(jobStat!=null){
        			//connect to file server
        			Stat fileStat = zkc.exists(fileServerPath, watcher); 
            		byte[] nodeInfo=zkc.getData(fileServerPath, fileStat);
                   	String conInfo=new String(nodeInfo);
                   	String[] host_Port=conInfo.split("_");
                   	socket=new Socket(host_Port[0],Integer.parseInt(host_Port[1]));
                   	out=new ObjectOutputStream(socket.getOutputStream());
                   	in=new ObjectInputStream(socket.getInputStream());
                   	
                   	
                   			
        			jobList=zkc.getChilds(jobPath, watcher);
        			if(jobList.size()==0)
        				break;
        			Random rn = new Random();
        			int jobListIndex=rn.nextInt()%jobList.size();
        			String joblistPath=jobList.get(jobListIndex);
        			String nodeNameSplit[]=joblistPath.split("_");
        			String hash=nodeNameSplit[0];
        			if(nodeNameSplit.length!=2){
        				continue;	
        			}
        			
        			//send a packet to server
        			output.type=workPacket.dictRequest;
        			output.index=Integer.parseInt(nodeNameSplit[1]);
        			out.writeObject(output);
        			
        			reply=(workPacket) in.readObject();
        			for(int i=0;i<reply.words.size();i++){
        				if(hash.equals(getHash(reply.words.get(i)))){
        					createNode(passSolved+"/"+hash+"_"+reply.words.get(i));
        					break;
        				}	
        			}
        			try
        			{
        			createNode(workDone+"/"+hash+"_"+nodeNameSplit[1]);
        			}catch(Exception e){
        				System.out.println("alry exist"+workDone+"/"+hash+"_"+nodeNameSplit[1]);
        			}
        			System.out.println("deleted?");
        			zkc.deleterNode(jobPath+"/"+hash+"_"+nodeNameSplit[1]);
        			in.close();
	                out.close();
	                socket.close();
        			

        		}else{
        			System.out.println("jobStat is null");
        		}
        		
    
        	}catch(Exception e){
        		//System.out.println("something wrong in worker");
        		try{
        			if(in!=null)
        				in.close();
        			if(in!=null)
        				out.close();
        			if(in!=null)
        				socket.close();
        		}catch(Exception e2){
        			//System.out.print("caught error closing sockets");
        		}
        		
        	}
        	
        }
    }

	private static void createNode(String path) {
		 Stat stat = zkc.exists(path, watcher);
	        if (stat == null) {              // znode doesn't exist; let's try creating it
	            System.out.println("Creating " + path);
	            Code ret = zkc.create(
	            			path,         // Path of znode
	                        null,           // Data not needed.
	                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
	                        );
	            if (ret == Code.OK) System.out.println("created node with path "+path);
	        } else{
	        	
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
