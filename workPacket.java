import java.io.Serializable;
import java.util.ArrayList;


public class workPacket implements Serializable {
	
	public static final int jobRquest=100;
	public static final int jobReply=200;
	
	public static final int dictRequest=300;
	public static final int dictReply=400;
	
	public int type=0;
	public int index=0;
	public ArrayList<String> words=new ArrayList<String>();

}
