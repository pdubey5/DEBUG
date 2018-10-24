import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option.Builder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Parser;


import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import java.util.Arrays;

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.lang.*;

public class Main {
	public static String prettyPrintJsonString(JsonNode jsonNode) {
    		try {
        		ObjectMapper mapper = new ObjectMapper();
        	 	Object json = mapper.readValue(jsonNode.toString(), Object.class);
        			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    		} catch (Exception e) {
        		return "Sorry, pretty print didn't work";
    		}
	}


	public static void assignOneSeek(String  TOPIC, KafkaConsumer<String, String>  consumer, Integer pollInterval , Integer partId, Integer seekOffset, Integer limit, String printStr) {
	
		Integer timeout = 0;
		Long oldOffset = 0L, newOffset=0L;
		List<TopicPartition> topicPartitions = new ArrayList();

	
		Integer numRec = 0, totRec = 0, print = 0, printMsg = 0;
		if (printStr.equals("print"))
        	       	print = 1;
		if (printStr.equals("printMsg")) {
        	       	print = 1;
        	       	printMsg = 1;
		}

		TopicPartition topicPar = new TopicPartition(TOPIC, partId);
		consumer.assign(Arrays.asList(topicPar));
		consumer.seek(topicPar, seekOffset);		
		while (true) 
		{
			ConsumerRecords<String,String> cr = consumer.poll(pollInterval);
   			numRec = cr.count();
			if ( numRec == 0)
				timeout++;
			System.out.printf("Got %d msgs in this poll\n", numRec);	
			for (ConsumerRecord<String,String> record : cr) {
				if (printMsg == 1 ) {
					try {
						ObjectMapper mapper = new ObjectMapper();
						JsonNode msg = mapper.readTree(record.value());
						System.out.printf("%s", prettyPrintJsonString(msg));	
					} catch (Exception e) {
						return;
					}
				}
				newOffset = record.offset();
				if ( print == 1) 
					System.out.printf("offset = %d\n", newOffset);
				if ( oldOffset + 1 != newOffset)
					System.out.printf("Not in seq, offset gap at Old  = %d, New = %d\n", oldOffset, newOffset);	
				oldOffset = newOffset;
				limit--;
				if (limit == 0)
					break;
			}
			if (limit == 0)
				break;
			totRec+=numRec;
			numRec=0;
			System.out.println("Total Records: " + totRec + "timeouts till now " + timeout) ;	
		}

        }

	public static void assignOne(String  TOPIC, KafkaConsumer<String, String>  consumer, Integer pollInterval, Integer partId, String printStr) {
	
		Integer timeout = 0;
		Long oldOffset = 0L, newOffset=0L;
		List<TopicPartition> topicPartitions = new ArrayList();

		Integer numRec = 0, totRec = 0, print = 0, printMsg = 0;
		if (printStr.equals("print"))
        	       	print = 1;
		if (printStr.equals("printMsg")) {
        	       	print = 1;
        	       	printMsg = 1;
		}

		consumer.assign(Arrays.asList(new TopicPartition(TOPIC, partId)));
		while (true) 
		{
			ConsumerRecords<String,String> cr = consumer.poll(pollInterval);
   			numRec = cr.count();
			if ( numRec == 0)
				timeout++;
			for (ConsumerRecord<String,String> record : cr) {
				if (printMsg == 1 ) {
					try {
						ObjectMapper mapper = new ObjectMapper();
						JsonNode msg = mapper.readTree(record.value());
						System.out.printf("%s", prettyPrintJsonString(msg));	
					} catch (Exception e) {
						return;
					}
				}
				newOffset = record.offset();
				if ( print == 1) 
					System.out.printf("offset = %d\n", newOffset);
				if ( oldOffset + 1 != newOffset)
					System.out.printf("Not in seq, offset gap at Old  = %d, New = %d\n", oldOffset, newOffset);	
				oldOffset = newOffset;
			}
			totRec+=numRec;
			numRec=0;
			System.out.println("Total Records: " + totRec + "timeouts till now " + timeout) ;	
		}

        }
	public static void gettimeout(String  TOPIC, KafkaConsumer consumer, Integer pollInterval, String args[]) {

		Integer numRec = 0, totRec = 0, timeout = 0;
		Long oldOffset = 0L, newOffset=0L;
		Integer print = 0, printMsg = 0;
		if (args[4].equals("print"))
			print = 1;
		if (args[4].equals("printMsg")) {
			print = 1;
			printMsg = 1;
		}

		consumer.subscribe(Arrays.asList(TOPIC));
		while (true) {
			ConsumerRecords<String,String> cr = consumer.poll(pollInterval);
   			numRec = cr.count();
			if ( numRec == 0)
				timeout++;
			for (ConsumerRecord<String,String> record : cr) {
				if (printMsg == 1 ) {
					try {
						ObjectMapper mapper = new ObjectMapper();
						JsonNode msg = mapper.readTree(record.value());
						System.out.printf("%s", prettyPrintJsonString(msg));	
					} catch (Exception e) {
						return;
					}
				}
				newOffset = record.offset();
				if ( print == 1) 
					System.out.printf("offset = %d\n", newOffset);
				if ( oldOffset + 1 != newOffset)
					System.out.printf("Not in seq, offset gap at Old  = %d, New = %d\n", oldOffset, newOffset);	
				oldOffset = newOffset;
			}
			System.out.printf("Adding %d records\n", numRec);
			totRec+=numRec;
			numRec=0;
			System.out.println("Total Records: " + totRec + "timeouts till now " + timeout) ;	
		}
        }
	public static void pollAll(String  TOPIC, KafkaConsumer consumer, Integer pollInterval, String printStr) {

		Integer numRec = 0, totRec = 0;
		Long oldOffset = 0L, newOffset=0L;
		Integer print = 0, printMsg = 0;
		if (printStr.equals("print"))
			print = 1;
		if (printStr.equals("printMsg")) {
			printMsg = 1;
			print = 1;
		}

		consumer.subscribe(Arrays.asList(TOPIC));
		while (true) {
			ConsumerRecords<String,String> cr = consumer.poll(pollInterval);
   			numRec = cr.count();
			if ( numRec == 0)
				return;
			for (ConsumerRecord<String,String> record : cr) {
				if (printMsg == 1 ) {
					try {
						ObjectMapper mapper = new ObjectMapper();
						JsonNode msg = mapper.readTree(record.value());
						System.out.printf("%s", prettyPrintJsonString(msg));	
					} catch (Exception e) {
						return;
					}
				}
				newOffset = record.offset();
				if ( print == 1) 
					System.out.printf("offset = %d\n", newOffset);
				if ( oldOffset + 1 != newOffset)
					System.out.printf("Not in seq, offset gap at Old  = %d, New = %d\n", oldOffset, newOffset);	
				oldOffset = newOffset;
			}
			consumer.commitSync();
			System.out.printf("Adding %d records\n", numRec);
			totRec+=numRec;
			numRec=0;
			System.out.println("Total Records: " + totRec ) ;	
		}
        }
	public static void assignAll (String  TOPIC, KafkaConsumer<String, String>  consumer, Integer pollInterval, String printStr) {
	
		Integer partId = 0;
		List<TopicPartition> topicPartitions = new ArrayList();
       		for (PartitionInfo pInfo : consumer.partitionsFor(TOPIC)) {
          		topicPartitions.add(new TopicPartition(TOPIC, pInfo.partition()));
        	}

		Integer numRec = 0, totRec = 0, print = 0, printMsg = 0;
		if (printStr.equals("print"))
        	       	print = 1;
		if (printStr.equals("printMsg")) {
			printMsg = 1;
			print = 1;
		}

		for (TopicPartition tp : topicPartitions) {
			List<TopicPartition> singlePartition = new ArrayList();
			singlePartition.add(tp);
			consumer.assign(singlePartition);
			while (true) 
			{
				ConsumerRecords<String,String> cr = consumer.poll(pollInterval);
				for (ConsumerRecord<String,String> record : cr) {
					numRec++;
				if (printMsg == 1 ) {
					try {
						ObjectMapper mapper = new ObjectMapper();
						JsonNode msg = mapper.readTree(record.value());
						System.out.printf("%s", prettyPrintJsonString(msg));	
					} catch (Exception e) {
						return;
					}
				}
					if ( print == 1) 
					System.out.printf("offset = %d\n", record.offset());
				}
				if (cr.isEmpty())
					break;
			}
			System.out.printf("Adding %d records\n", numRec);
			totRec+=numRec;
			numRec=0;
			System.out.println("Consumer Position is :"+ consumer.position(tp));	
			System.out.println("Total Records: " + totRec ) ;	
		}

    }
    public static void main(String[] args) {

	Options options = new Options();
	Option stream = new Option("s", "stream", true, "Stream & TopicName : /<StreamPath>:<TopicName>");
	stream.setRequired(true);
	options.addOption(stream);

	Option module = new Option("m", "module", true, "Module Name (consumer/producer)");
	module.setRequired(true);
	options.addOption(module);

	Option reset = new Option("r", "reset", true, "AutoOffset Reset value (earliest,latest) Default: earliest");
	reset.setRequired(false);
	options.addOption(reset);

	Option gId = new Option("g", "grId", true, "Group Id (GroupID) Default:tmp");
	gId.setRequired(false);
	options.addOption(gId);

	Option pollInt = new Option("t", "pollInt", true, "Poll Interval Default: 500");
	pollInt.setRequired(false);
	options.addOption(pollInt);

	Option func = new Option("f", "func", true, "Consumer Function (poll/assignPoll/assignOne/gettimeout/assignOneSeek) Default: poll");
	func.setRequired(false);
	options.addOption(func);

	Option  data = new Option("d", "data", true, "Weather to print Data or only count number of messages(default)  (print/noprint/printMsg) Default:noprint");
	data.setRequired(false);
	options.addOption(data);

	Option num = new Option("n", "num", true, "Number of Messages to produce (<number>) Default 100");
	num.setRequired(false);
	options.addOption(num);

	Option partId = new Option("p", "partId", true, "Partition Id (<number>) Default 0");
	partId.setRequired(false);
	options.addOption(partId);

	Option offset = new Option("o", "offset", true, "Seek Offset for function assignOneSeek (<number>) Default 0");
	offset.setRequired(false);
	options.addOption(offset);

	Option commit = new Option("c", "commit", true, "Enable Auto Commit ? (true/false) Default true");
	commit.setRequired(false);
	options.addOption(commit);

	Option limit = new Option("l", "limit", true, " No of messages to print from Offset specified with -o (<number>) Default 500");
	limit.setRequired(false);
	options.addOption(limit);

	CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
	
	try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Mapr Stream Debugger\n" +
		"java -cp .:./mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -topic testSpring:topic1 -p producer -n 4000\n" +
		"java -cp .:./target/mapr-streams-debug-1.0-jar-with-dependencies.jar:`mapr classpath` Main  -topic testSpring:topic1 -p  consumer  -a earliest -i 500 -g trp -f poll -o print\n"
		, options);
            System.exit(1);
        }

        String TOPIC = "/" + cmd.getOptionValue("stream");
	String program = cmd.getOptionValue("module");
	System.out.println("Topic is " + TOPIC);
        if (program.equals("consumer"))
	{
		Integer pollInterval = 500;
		System.out.println("Topic to be queried is:" +  TOPIC);
		Properties props = new Properties();
		props.put("bootstrap.servers", "");
		props.put("auto.offset.reset", cmd.getOptionValue("reset","earliest"));
		props.put("enable.auto.commit", cmd.getOptionValue("commit","true"));
		props.put("max.poll.records", Long.parseLong(cmd.getOptionValue("num","500")));
		System.out.print("Group ID is " + cmd.getOptionValue("grId","tmp"));
		props.put("group.id", cmd.getOptionValue("grId","tmp"));
		props.put("auto.commit.interval.ms", "100");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("max.partition.fetch.bytes", Integer.MAX_VALUE);

		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);

		pollInterval=Integer.parseInt(cmd.getOptionValue("pollInt", "500"));
		String funct = cmd.getOptionValue("func","poll");	
		if (funct.equals("poll"))
			pollAll (TOPIC, consumer , pollInterval, cmd.getOptionValue("data","noprint"));
		else if (funct.equals("assignPoll"))
			assignAll (TOPIC, consumer, pollInterval, cmd.getOptionValue("data","noprint"));
		else if (funct.equals("assignOne"))
			assignOne (TOPIC, consumer, pollInterval, Integer.parseInt(cmd.getOptionValue("partId","0")), cmd.getOptionValue("data","noprint"));
		else if (funct.equals("gettimeout"))
			gettimeout (TOPIC, consumer, pollInterval, args);
		else if (funct.equals("assignOneSeek"))
			assignOneSeek(TOPIC, consumer, pollInterval, Integer.parseInt(cmd.getOptionValue("partId","0")), Integer.parseInt(cmd.getOptionValue("offset","0")), Integer.parseInt(cmd.getOptionValue("limit","0")), cmd.getOptionValue("data","noprint"));
	} else if (program.equals("producer"))
	{
		Long numMessages = 0L;
		System.out.println(TOPIC);
		try {
			numMessages = Long.parseLong(cmd.getOptionValue("num","100"));
		}
		catch (NumberFormatException nfe) {
			System.out.println("The first argument must be an integer.");
            		System.exit(1);
        	}
		KafkaProducer<String, String> producer;
		Properties props = new Properties();
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("block.on.buffer.full", "true");
		producer = new KafkaProducer<>(props);
		System.out.println("Entering Produce with number of message to produce: " + numMessages);
		try {
			for (int i = 0; i < numMessages; i++) {
				System.out.println("Send after Entering Produce");
				producer.send(new ProducerRecord<String, String>(
                        			TOPIC,
                        			String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
				System.out.println("Sent msg number " + i);
			}
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			producer.close();
		}
	}
	return;
    }
}
