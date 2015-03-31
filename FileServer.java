import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class FileServer {
    
    String myPath = "/fileServer";
    ZkConnector zkc;
    Watcher watcher;
    static String host;
    static int port;
    public static ArrayList<String> password=new ArrayList<String>();

    public static void main(String[] args) {
      
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort");
            return;
        }

        try{
        	host=InetAddress.getLocalHost().getHostName();
        	port=Integer.parseInt(args[1]);
	        BufferedReader br = new BufferedReader(new FileReader("dictionary/lowercase.rand"));
	        String line;
	        while ((line = br.readLine()) != null) {
	        	password.add(line);
	        }
	        FileServer t = new FileServer(args[0]);
	        
	        t.checkpath();
	        
	        ServerSocket serverSocket = new ServerSocket(port);
	        System.out.println("password size is : "+password.size());  
            while (true) {
                Socket socket = serverSocket.accept();
                new FileServerThread(socket).start();
            }
        }
        catch(Exception e){
        	
        }
       
    }

    public FileServer(String hosts) {
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
    
    private void checkpath() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        host+"_"+port,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("the boss fileserver now");
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
                //try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

}
class FileServerThread extends Thread {
	Socket socket=null;
	ObjectInputStream in =null;
	ObjectOutputStream out=null;
	
	FileServerThread(Socket _socket){
		super();
		try{
			this.socket=_socket;
			this.in=new ObjectInputStream (this.socket.getInputStream());
			this.out=new ObjectOutputStream (this.socket.getOutputStream());
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public void run(){
		try {
			workPacket packet=(workPacket)in.readObject();
			System.out.println("getting some of dem packet hmmm hmmm");  
			if(packet.type==workPacket.dictRequest){
				
				workPacket replyPacket=new workPacket();
				
				int firstIndex=packet.index*1000;
				int lastIndex;
				if(firstIndex+1000<FileServer.password.size())
					lastIndex=firstIndex+1000;
				else
					lastIndex=FileServer.password.size()-1;
					
				for(int i=firstIndex;i<=lastIndex;i++){
					replyPacket.words.add(FileServer.password.get(i));
				}
				
				replyPacket.type=workPacket.dictReply;
				out.writeObject(replyPacket);
			}
			in.close();
            out.close();
            socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
			
		}
	}
	
}

