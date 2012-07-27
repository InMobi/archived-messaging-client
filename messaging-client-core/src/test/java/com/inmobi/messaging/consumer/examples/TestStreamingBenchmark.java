package com.inmobi.messaging.consumer.examples;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.MockConsumer;

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
}
