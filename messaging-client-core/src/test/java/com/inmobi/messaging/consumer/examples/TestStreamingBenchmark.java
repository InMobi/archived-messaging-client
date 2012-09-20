package com.inmobi.messaging.consumer.examples;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.MockConsumer;
import com.inmobi.messaging.publisher.MockPublisher;

public class TestStreamingBenchmark {

  @Test
  public void testExitcodes() throws Exception {
    List<String> args = new ArrayList<String>();

    int exitcode;
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.WRONG_USAGE_CODE);

    // test producer exitcodes for number of arguments
    args.add("-producer");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.WRONG_USAGE_CODE);

    args.add("test");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.WRONG_USAGE_CODE);

    args.add("10");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.WRONG_USAGE_CODE);

    args.add("1000");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, 0);

    args.add("10");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, 0);

    //test producer for wrong number of messages per sec
    args.clear();
    args.add("-producer");
    args.add("test");
    args.add("100");
    args.add("0");
    Throwable th = null;
    try {
      exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    } catch (Throwable e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    //test producer for wrong number of total messages
    args.clear();
    args.add("-producer");
    args.add("test");
    args.add("0");
    args.add("100");
    th = null;
    try {
      exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    } catch (Throwable e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    //test producer for timeout seconds
    args.clear();
    args.add("-producer");
    args.add("test");
    args.add("100");
    args.add("10");
    args.add("1");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.FAILED_CODE);
  }

  @Test
  public void testConsumerExitCodes() throws Exception {
    List<String> args = new ArrayList<String>();
    int exitcode;

    // test consumer exitcodes for number of arguments
    args.add("-consumer");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.WRONG_USAGE_CODE);

    args.add("1");
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.WRONG_USAGE_CODE);

    args.add("1"); // 1 message
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.FAILED_CODE);

    MockConsumer.block = true;
    args.clear();
    args.add("-consumer");
    args.add("1");
    args.add("10"); // 10 messages
    args.add("1"); // 1 second timeout
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    Assert.assertEquals(exitcode, StreamingBenchmark.FAILED_CODE);
    MockConsumer.block = false;
  }

  @Test
  public void testProducerMsgSize() throws Exception {
    List<String> args = new ArrayList<String>();
    int exitcode;
    int msgSize = 100;
    String topic = "testMsgSize";
    long numMessages = 10;
    args.add("-producer");
    args.add(topic);
    args.add(Long.toString(numMessages));
    args.add("10000");
    args.add("1");
    args.add(Integer.toString(msgSize));
    MockPublisher.reset(); 
    exitcode = StreamingBenchmark.run(args.toArray(new String[0]));
    String[] msg = StreamingBenchmark.getMessage(MockPublisher.getMsg(topic),
        false).split(StreamingBenchmark.DELIMITER);
    Assert.assertEquals(exitcode, 0);
    Assert.assertEquals(msg.length, 3);
    Assert.assertEquals(Long.parseLong(msg[0]), numMessages);
    System.out.println("msg:" + msg[2]);
    Assert.assertEquals(msg[2].length(), msgSize);
    Assert.assertEquals(msg[2], new String(StreamingBenchmark.getMessageBytes(
        msgSize)));
    MockPublisher.reset(); 
  }
}
