# StreamsDebug

# MapR Streams Debug jar

# usage:
 -s,--stream <arg>    Stream & TopicName : /<StreamPath>:<TopicName>.
 -m,--module <arg>    Module Name (consumer/producer).
 -n,--num <arg>       Number of Messages to produce (<number>) Default 100.
 -f,--func <arg>      Consumer Function 
                      (poll/assignPoll/assignOne/gettimeout/assignOneSeek)
                      Default: poll
 -c,--commit <arg>    Enable Auto Commit ? (true/false) Default true
 -d,--data <arg>      Weather to print Data also or only count number of
                      messages(noprint)  (print/noprint/printMsg)
                      Default:noprint, print will print only offsets
 -g,--grId <arg>      Group Id (GroupID) Default:tmp
 -l,--limit <arg>      No of messages to print from Offset specified with
                      -o (<number>) Default 500
 -o,--offset <arg>    Seek Offset for function assignOneSeek (<number>)
                      Default 0
 -p,--partId <arg>    Partition Id (<number>) Default 0
 -r,--reset <arg>     AutoOffset Reset value (earliest,latest) Default:
                      earliest
 -t,--pollInt <arg>   Poll Interval Default: 500


Sample Run Commands:

Produce Messages to the Stream
java -cp .:target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s /testSpring:topic1 -m producer -n 1

Simply Consumer with default options for consumer:
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s testSpring:topic1 -m  consumer

Specify the Consumer Group and Consumer
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s testSpring:topic1 -m  consumer -g temp1

Assign the consumer to each partitions and then poll on that partitions and check messages in each partitions
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s /strDbg:topic1 -m  consumer -g temp1 -f assignPoll

Assign the consumer to a specific partitions and then poll on that partition and check messages 
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s /strDbg:topic1 -m  consumer -g grp  -f assignOne -p 0

