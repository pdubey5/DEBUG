```
# StreamsDebug
This program can be used to run and check messages and offsets in a stream and also do operations only on particular topic partition in respect to consumer.

You can do these tasks with this:
1. Produce Messages to the Stream.
2. Run a Consumer and consume and print offset or/and messages.
3. Count Number of Messages within a stream.
4. Specify the Consumer Group and then run a Consumer
5. Assign the consumer to each partitions and then poll on that partitions and check messages in each partitions
6. Assign the consumer to a specific one partitions and then poll on that partition and check messages 

You need to have these packages installed on the client:
For 6.0.1
mapr-kafka-1.1.1.201806260740-1.noarch
mapr-librdkafka-0.11.3.201803231414-1.noarch

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

Simply Run a Consumer with default options for consumer:
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s testSpring:topic1 -m  consumer

Count Number of Messages within a stream:
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s /strDbg:topic1 -m  consumer -g gcount -f poll

Specify the Consumer Group and then run a Consumer
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s testSpring:topic1 -m  consumer -g temp1

Assign the consumer to each partitions and then poll on that partitions and check messages in each partitions
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s /strDbg:topic1 -m  consumer -g temp1 -f assignPoll

Assign the consumer to a specific one partitions and then poll on that partition and check messages 
java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -s /strDbg:topic1 -m  consumer -g grp  -f assignOne -p 0


```
