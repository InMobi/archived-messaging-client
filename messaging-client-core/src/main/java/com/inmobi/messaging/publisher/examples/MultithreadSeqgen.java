package com.inmobi.messaging.publisher.examples;

import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

/**
 * Sends messages from multiple threads. 
 *
 */
public class MultithreadSeqgen {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MultithreadSeqGeneratorClient <topic>" +
      		" <maxSeq> <number_of_threads>");
      return;
    }
    AbstractMessagePublisher publisher = 
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    String topic = args[0];
    long maxSeq = Integer.parseInt(args[1]);
    int totalThread = Integer.parseInt(args[2]);
    PublishThreadNew[] p = new PublishThreadNew[totalThread];
    long threadMaxSeq = maxSeq / totalThread ;
    for(int thread = 0; thread < totalThread; thread++){
      p[thread] = new PublishThreadNew(topic, publisher,threadMaxSeq);
      p[thread].start();
    }
    for(int thread = 0; thread < totalThread ; thread++){
      p[thread].join();
    }

    SeqGeneratorClient.waitToComplete(publisher, topic);
    publisher.close();
    long invocation = publisher.getStats(topic).getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: " +
        publisher.getStats(topic).getSuccessCount());
    System.out.println("Total unhandledExceptions: " +
        publisher.getStats(topic).getUnhandledExceptionCount());
  }

  private static class PublishThreadNew extends Thread {
    private String topic;
    private AbstractMessagePublisher publisher;
    private long maxSeq;

    PublishThreadNew(String topic,
        AbstractMessagePublisher publisher, long maxSeq) {
      this.topic = topic;
      this.publisher = publisher;
      this.maxSeq = maxSeq;
    }

    public void run() {
      try {
        SeqGeneratorClient.publishMessages(publisher, topic, maxSeq);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
