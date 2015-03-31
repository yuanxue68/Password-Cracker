import java.io.Serializable;
import java.util.ArrayList;


public class workPacket implements Serializable {
	
	public static final int jobRequest=100;
	public static final int jobReply=200;
	
	public static final int jobQuery=300;
	public static final int jobQueryReply=400;
	
	public static final int dictRequest=500;
	public static final int dictReply=600;
	
	//public static final int jobExist=1000;
	public static final String jobNotExist="Job Does Not Exist";
	public static final String jobExist="Job Exist";
	public static final String jobInProgress="Job In Progress";
	public static final String jobFinishedNotFound="Job FInished Password Not Found";
	public static final String jobFInishedFound="Job Finished Password is: ";
	
	public String ReplyMsg;
	public String hashedPassword;
	public int type=0;
	public int index=0;
	public ArrayList<String> words=new ArrayList<String>();

}
