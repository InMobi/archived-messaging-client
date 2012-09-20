package com.inmobi.messaging.logger;

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.testng.annotations.Test;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MockPublisher;

public class TestLogger {

  @Test
  public void test() throws TException {
    PropertyConfigurator.configure("src/test/resources/log4j.properties");
    Logger logger = Logger.getLogger("messagingclient");
    MessageAppender appender = (MessageAppender) logger
        .getAppender("messagingclient");
    Assert.assertNull(appender.getConffile());
    doTest(logger, appender);
  }

  @Test
  public void testWithConf() throws TException {
    PropertyConfigurator
        .configure("src/test/resources/log4j-with-conf.properties");
    Logger logger = Logger.getLogger("messagingclient");
    MessageAppender appender = (MessageAppender) logger
        .getAppender("messagingclient");
    Assert.assertEquals(appender.getConffile(),
        "src/test/resources/messaging-publisher-conf.properties");
    doTest(logger, appender);
  }

  private void doTest(Logger logger, MessageAppender appender)
      throws TException {
    String topic = "test";
    Assert.assertEquals(appender.getTopic(), topic);
    Message msg = new Message(ByteBuffer.wrap("hello".getBytes()));
    logger.info(msg);
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);

    // test byte[] logging
    logger.info(msg.getData().array());
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);

    // test String logging
    logger.info(new String(msg.getData().array()));
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);

    // test Object logging. any other kind must be ignored
    logger.info(new Object());
    Assert.assertNull(MockPublisher.getMsg(topic));
    MockPublisher.reset(topic);

    // test thrift object logging
    LogEntry le = new LogEntry();
    le.category = "xxxx";
    le.message = "massage";
    logger.info(le);
    TSerializer serializer = new TSerializer();
    Assert.assertEquals(MockPublisher.getMsg(topic), 
        new Message(serializer.serialize(le)));
    MockPublisher.reset(topic);
  }
}
