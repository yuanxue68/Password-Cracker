build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ClientDriver.java FileServer.java JobTracker.java JobTrackerThread.java Worker.java workPacket.java ZkConnector.java
clean:
	rm -f *.class
