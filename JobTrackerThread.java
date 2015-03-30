import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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


public class JobTrackerThread extends Thread{
	Socket socket=null;
	ObjectInputStream in = null;
	ObjectOutputStream out =null;
	
	String myPath = "/jobs";
    ZkConnector zkc=new ZkConnector();;
    Watcher watcher;

	public JobTrackerThread(String trackerHost,Socket _socket) {
		super();
		try{
			zkc.connect(trackerHost);
			this.socket=_socket;
			this.in=new ObjectInputStream (this.socket.getInputStream());
			this.out=new ObjectOutputStream (this.socket.getOutputStream());
			
			 watcher = new Watcher() { // Anonymous Watcher
                 @Override
                 public void process(WatchedEvent event) {
                     handleEvent(event);
             
                 } };

		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public void run(){
		try{
			workPacket packet=(workPacket) in.readObject();
			System.out.println("getting some of dem jobtrackerthead packet hmmm hmmm"); 
			
			workPacket replyPacket=new workPacket();
			replyPacket.type=workPacket.jobReply;
			String hash=packet.hashedPassword;
			
			if(packet.type==workPacket.jobRequest){
				 Stat stat = zkc.exists("/jobs/job_" +hash, watcher);
	             if (stat == null) {
	                 replyPacket.type = workPacket.jobExist;
	                 out.writeObject(replyPacket);
	             }else{
	            	 
	            	 createJob(hash);
	            	 out.writeObject(replyPacket);
	             }
				
			}else if(packet.type==workPacket.jobQuery){
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	
	private void createJob(String hash) {
		// TODO Auto-generated method stub
		
	}

	private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                //checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                //try{ Thread.sleep(5000); } catch (Exception e) {}
                //checkpath(); // re-enable the watch
            }
        }
    }
	
	/*private void checkpath() {
	Stat stat = zkc.exists(myPath, watcher);
	    if (stat == null) {              // znode doesn't exist; let's try creating it
	    	System.out.println("Creating " + myPath);
	        Code ret = zkc.create(
	        			myPath,         // Path of znode
	                    null,           // Data not needed.
	                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
	                    );
	        if (ret == Code.OK) System.out.println("the boss fileserver now");
	    } 
	}*/

}
