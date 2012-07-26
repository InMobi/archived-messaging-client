package com.inmobi.messaging.consumer.examples;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.util.ConsumerUtil;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;

public class StreamingBenchmark {

  static final String DELIMITER = "/t";
  static final SimpleDateFormat LogDateFormat = new SimpleDateFormat(
      "yyyy:MM:dd hh:mm:ss");

  static void printUsage() {
    System.out.println(
        "Usage: StreamingBenchmark  " +
        " [-producer <topic-name> <no-of-msgs> <no-of-msgs-per-sec> ]" +
        " [-consumer <no-of-producers> <no-of-msgs> [<hadoopconsumerflag>] [<timezone>]]");
    System.exit(-1);    
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage();
    }
    long maxSent = -1;
    int numMsgsPerSec = -1;
    String timezone = null;
    String topic = null;
    int numProducers = 1;
    boolean runProducer = false;
    boolean runConsumer = false;
    boolean hadoopConsumer = false;

    if (args.length >= 3) {
      int consumerOptionIndex = -1;
      if (args[0].equals("-producer")) {
        topic = args[1];
        maxSent = Long.parseLong(args[2]);
        numMsgsPerSec = Integer.parseInt(args[3]);
        runProducer = true;
        consumerOptionIndex = 4;
      } else {
        consumerOptionIndex = 0;
      }
      
      if (args.length > consumerOptionIndex) {
        if (args[consumerOptionIndex].equals("-consumer")) {
          numProducers = Integer.parseInt(args[consumerOptionIndex + 1]);
          maxSent = Long.parseLong(args[consumerOptionIndex + 2]);
          if (args.length > consumerOptionIndex + 3) {
            hadoopConsumer = (Integer.parseInt(args[consumerOptionIndex + 3]) > 0);
          }
          if (args.length > consumerOptionIndex + 4) {
            timezone = args[consumerOptionIndex + 4];
          }
          runConsumer = true;
        }
      }
    } else {
      printUsage();
    }

    assert(runProducer || runConsumer == true);
    Producer producer = null;
    Consumer consumer = null;
    StatusLogger statusPrinter;

    if (runProducer) {
     System.out.println("Using topic: " + topic);
      producer = createProducer(topic, maxSent, numMsgsPerSec);
      producer.start();
    }
    
    if (runConsumer) {
      ClientConfig config = ClientConfig.loadFromClasspath(
          MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
      Date now;
      if (timezone != null) {
        now = ConsumerUtil.getCurrenDateForTimeZone(timezone);
      } else {
        now = Calendar.getInstance().getTime(); 
      }
      System.out.println("Starting from " + now);

      // create and start consumer
      assert(config != null);
      consumer = createConsumer(config, maxSent, now, numProducers, hadoopConsumer);
      consumer.start();
    }
    
    statusPrinter = new StatusLogger(producer, consumer);
    statusPrinter.start();

    
    if (runProducer) {
      assert (producer != null);
      producer.join();
      if (!runConsumer) {
        statusPrinter.stopped = true;
      }
    } 
    if (runConsumer) {
      assert (consumer !=null);
      consumer.join();
      statusPrinter.stopped = true;
    }

    statusPrinter.join();
    if (runConsumer) {
      if (!consumer.success) {
        System.out.println("Data validation FAILED!");
      } else {
        System.out.println("Data validation SUCCESS!");
      }
    }
    System.exit(0);
  }

  static Producer createProducer(String topic, long maxSent, int numMsgsPerSec)
      throws IOException {
    return new Producer(topic, maxSent, numMsgsPerSec); 
  }

  static Consumer createConsumer(ClientConfig config, long maxSent,
      Date startTime, int numProducers, boolean hadoopConsumer) throws IOException {
    return new Consumer(config, maxSent, startTime, numProducers, hadoopConsumer);    
  }

  static class Producer extends Thread {
    volatile AbstractMessagePublisher publisher;
    final String topic;
    final long maxSent;
    final long sleepMillis;
    final long numMsgsPerSleepInterval;

    Producer(String topic, long maxSent, int numMsgsPerSec) throws IOException {
      this.topic = topic;
      this.maxSent = maxSent;
      if (maxSent <= 0) {
        throw new IllegalArgumentException("Invalid total number of messages");
      }
      if (numMsgsPerSec > 1000) {
        this.sleepMillis = 1;
        numMsgsPerSleepInterval= numMsgsPerSec/1000;        
      } else {
        if (numMsgsPerSec <= 0) {
          throw new IllegalArgumentException("Invalid number of messages per" +
          		" second");
        }
        this.sleepMillis = 1000/numMsgsPerSec;
        numMsgsPerSleepInterval = 1;
      }
      publisher = (AbstractMessagePublisher) MessagePublisherFactory.create();
    }

    @Override
    public void run() {
      System.out.println("Producer started!");
      long i = 1;
      boolean sentAll= false;
      while (true) {
        for (long j = 0; j < numMsgsPerSleepInterval; j++) {
          long time = System.currentTimeMillis();
          String s = i + DELIMITER + Long.toString(time);
          Message msg = new Message(ByteBuffer.wrap(s.getBytes()));
          publisher.publish(topic, msg);
          if (i == maxSent) {
            sentAll = true;
            break;
          }
          i++;
        }
        if (sentAll) {
          break;
        }
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
      }
      // wait for complete
      while (publisher.getStats().getInFlight() > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return;
      }

      publisher.close();
      System.out.println("Producer closed");
    }

  }

  static class Consumer extends Thread {
    //volatile Set<Long> seqSet = new HashSet<Long>();
    final Map<Long, Integer> messageToProducerCount;
    final MessageConsumer consumer;
    final long maxSent;
    volatile long received = 0;
    volatile long totalLatency = 0;
    int numProducers;
    boolean success = false;
    boolean hadoopConsumer = false;

    Consumer(ClientConfig config, long maxSent, Date startTime,
        int numProducers, boolean hadoopConsumer) throws IOException {
      this.maxSent = maxSent;
      messageToProducerCount = new HashMap<Long, Integer>((int)maxSent);
      this.numProducers = numProducers;
      consumer = MessageConsumerFactory.create(config, startTime);
      this.hadoopConsumer = hadoopConsumer;
    }

    private String getMessage(Message msg) throws IOException {
      byte[] byteArray = msg.getData().array();
      if (!hadoopConsumer) {
        return new String(byteArray);
      } else {
        Text text = new Text();
        ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
        text.readFields(new DataInputStream(bais));
        return new String(Base64.decodeBase64(text.getBytes()));
      }
    }
    @Override
    public void run() {
      System.out.println("Consumer started!");
      while (true) {
        Message msg = null;
        try {
          msg = consumer.next();
          received++;
          String s = getMessage(msg);
          String[] ar = s.split(DELIMITER);
          Long seq = Long.parseLong(ar[0]);
          Integer pcount = messageToProducerCount.get(seq);
          if (pcount == null) {
            messageToProducerCount.put(seq, new Integer(1));
          } else {
            pcount++;
            messageToProducerCount.put(seq, pcount);
          }
          long sentTime = Long.parseLong(ar[1]);
          totalLatency += System.currentTimeMillis() - sentTime;
          if (received == maxSent * numProducers) {
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        } catch (Exception e) {
          System.out.println("Got exception for " + 
            new String(msg.getData().array()));
          e.printStackTrace();
        }
      }
      if (messageToProducerCount.size() == maxSent) {
        for (Integer pcount : messageToProducerCount.values()) {
          if (pcount != numProducers) {
            success = false;
            break;
          } else {
            success = true;
          }
        }
      } else {
        success = false;
      }
      consumer.close();
      System.out.println("Consumer closed");
    }

  }

  static class StatusLogger extends Thread {
    volatile boolean stopped;
    Producer producer;
    Consumer consumer;
    StatusLogger(Producer producer, Consumer consumer) {
      this.producer = producer;
      this.consumer = consumer;
    }
    @Override
    public void run() {
      while(!stopped) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
        StringBuffer sb = new StringBuffer();
        sb.append(LogDateFormat.format(System.currentTimeMillis()));
        if (producer != null) {
          constructProducerString(sb);
        }
        if (consumer != null) {
          constructConsumerString(sb);
        }
        System.out.println(sb.toString());
      }
    }
    
    void constructProducerString(StringBuffer sb) {
      sb.append(" Invocations:" + producer.publisher.getStats().
          getInvocationCount());
      sb.append(" Inflight:" + producer.publisher.getStats().getInFlight());
      sb.append(" SentSuccess:" + producer.publisher.getStats().
          getSuccessCount());
      sb.append(" UnhandledExceptions:" + producer.publisher.getStats().
          getUnhandledExceptionCount());      
    }
    
    void constructConsumerString(StringBuffer sb) {
      sb.append(" Received:" + consumer.received);
      sb.append(" UniqueReceived:");
      sb.append(consumer.messageToProducerCount.size());
      if (consumer.received != 0) {
        sb.append(" MeanLatency(ms):" 
            + (consumer.totalLatency / consumer.received));
      }      
    }
  }
}
