# Password-Cracker
a password cracker with Apache Zookeeper

Code need some clean up on all demmm sys out

# How to Run:
1) Replace "8855" with your own port that Zookeeper is running on.  
2) Reference your own jar path and dictionary file.  
3) Run runJobTracker, runFileServer, runWorker in that order, each with at least two copies (for fault tolerence).  
4) Finally, run the client.  
5) Client will return "In progress" once the hash is successfully submitted.

**runJobTracker:**  
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker localhost:8855  

**runFileServer:**  
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer localhost:8855 **./dictionary/lowercase.rand**  

**runWorker:**  
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker localhost:8855  
		
**runClient:**  
	**To submit job:** java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver localhost:8855 job **hash**  
	**To query job:** java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver localhost:8855 status **hash**  
