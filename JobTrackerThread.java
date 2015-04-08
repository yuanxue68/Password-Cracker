import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;

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
			System.out.println("creating new job thread on host "+trackerHost);
			zkc.connect(trackerHost);
			System.out.println("zk connected");
			this.socket=_socket;
			//System.out.println("start to createstream in job tracker thread");
			this.in=new ObjectInputStream (_socket.getInputStream());
			this.out=new ObjectOutputStream (_socket.getOutputStream());
			//System.out.println("created socket and stream");
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
			//System.out.println("getting some of dem jobtrackerthead packet hmmm hmmm"); 
			
			workPacket replyPacket=new workPacket();
			String hash=packet.hashedPassword;
			String path=myPath+"/"+hash;
			
			if(packet.type==workPacket.jobRequest){
				replyPacket.type=workPacket.jobReply;
				 Stat stat = zkc.exists(path, watcher);
	             if (stat != null) {
	                 replyPacket.type = workPacket.jobReply;
	                 replyPacket.ReplyMsg=workPacket.jobExist;
	                 out.writeObject(replyPacket);
	             }else{
	            	 createJob(hash);
	            	 replyPacket.ReplyMsg=workPacket.jobInProgress;
	            	 out.writeObject(replyPacket);
	             }
				
			}else if(packet.type==workPacket.jobQuery){
				Stat stat=zkc.exists(path, watcher);
				boolean done =false;
				int doneSegSize=0;
				if(stat==null){
					replyPacket.type=workPacket.jobQueryReply;
					replyPacket.ReplyMsg=workPacket.jobNotExist;
				}else{
					//266 is the max number of segments of 1000 words
					ArrayList<String> list=new ArrayList<String>();
					list=zkc.getChilds("/jobs", watcher);
					System.out.println(hash+" list size is "+list.size());
					for(int i=0;i<list.size();i++){
						if(list.get(i).split("_")[0].equals(hash))
						doneSegSize++;
					}
					if(doneSegSize==1)
						done=true;
					
					if(done){
						boolean solved=false;
						String password="";
						ArrayList<String> solvedList=new ArrayList<String>();
						solvedList=zkc.getChilds("/passSolved", watcher);
						for(int i=0;i<solvedList.size();i++){
							String[] temp=solvedList.get(i).split("_");
							if(temp[0].equals(hash)){
								solved=true;
								password=temp[1];
							}
						}
						replyPacket.type=workPacket.jobQueryReply;
						if(solved)
							replyPacket.ReplyMsg=workPacket.jobFInishedFound+password;
						else
							replyPacket.ReplyMsg=workPacket.jobFinishedNotFound;
						
					}else{
						replyPacket.type=workPacket.jobQueryReply;
						replyPacket.ReplyMsg=workPacket.jobInProgress;
					}
				}
				out.writeObject(replyPacket);
				
			}
			in.close();
            out.close();
            socket.close();
            zkc.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}
	
	
	private void createJob(String hash) {
		//first check if /job path exits
		String path=myPath;
		Stat stat = zkc.exists(path, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            //System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        path,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            //if (ret == Code.OK)
            	//System.out.println(myPath+"created");
        } 
        //create a node to track finished segments of the hash
        path="/done/"+hash;
        Stat stat2 = zkc.exists(path, watcher);
        if (stat2 == null) {              // znode doesn't exist; let's try creating it
            //System.out.println("Creating " + path);
            Code ret = zkc.create(
                        path,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            //if (ret == Code.OK) System.out.println("/workDone/"+hash+"created");
        } 
       
        
        Stat stat4=null;
        for(int i=0;i<266;i++){
        	//create path such as : jobs/hash_230
        	path=myPath+"/"+hash+"_"+i;
        	stat4 = zkc.exists(path, watcher);
        	if(stat4==null){
        		//System.out.println("Creating " + myPath+"/"+hash+"_"+i);
	        	Code ret = zkc.create(
	                    path,         // Path of znode
	                    null,           // Data not needed.
	                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
	                    );
	        	//if (ret == Code.OK) System.out.println(path+" created");
        	}
        	stat4=null;
        	
        }
        
        path=myPath+"/"+hash;
        Stat stat3=zkc.exists(path, watcher);
        if(stat3==null){
        	//System.out.println("Creating " + path);
            Code ret = zkc.create(
            			path,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            //if (ret == Code.OK) System.out.println(path+" created");
        	
        }
        
		
		
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
