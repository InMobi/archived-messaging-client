package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.stats.MockStatsEmitter;
import com.inmobi.stats.emitter.EmitMondemand;

public class TestPublisher {

  @Test
  public void test() throws IOException {
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf);
    doTest(publisher);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));
  }

  @Test
  public void testLoadFromClasspath() throws IOException {
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue((
        (MockStatsEmitter)publisher.getMetrics().getStatsEmitter()).inited);
  }

  @Test
  public void testLoadFromFileName() throws IOException {
    URL url = getClass().getClassLoader().getResource(
        MessagePublisherFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(
            url.getFile());
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue((
        (MockStatsEmitter)publisher.getMetrics().getStatsEmitter()).inited);
  }

  @Test
  public void testLoadFromClassName() throws IOException {
    ClientConfig conf = new ClientConfig();
    AbstractMessagePublisher publisher = 
      (AbstractMessagePublisher) MessagePublisherFactory.create(
          conf, MockPublisher.class.getName());
    doTest(publisher);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));
  }

  @Test
  public void testMondemand() throws IOException {
    ClientConfig conf = new ClientConfig();
    URL url = getClass().getClassLoader().getResource(
        "mondemand-emitter.properties");
    conf.set(MessagePublisherFactory.EMITTER_CONF_FILE_KEY, url.getFile());
    AbstractMessagePublisher publisher = 
      (AbstractMessagePublisher) MessagePublisherFactory.create(
          conf, MockPublisher.class.getName());
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue((
        publisher.getMetrics().getStatsEmitter()) instanceof EmitMondemand);    
  }

  private void doTest(AbstractMessagePublisher publisher) {
    String topic1 = "test1";
    String topic2 = "test2";
    doTest(topic1, publisher);
    doTest(topic2, publisher);
    publisher.close();
  }

  private void doTest(String topic, AbstractMessagePublisher publisher) {
    Throwable th = null;
    String nullTopic = null;
    // publish null message
    try {
      publisher.publish(topic, null);
    } catch (Throwable t) {
      th = t;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
    Message msg = new Message( ByteBuffer.wrap(new byte[5]));

    th = null;
    // publish to null topic
    try {
      publisher.publish(nullTopic, msg);
    } catch (Throwable t) {
      th = t;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
    
    // publish the message
    Assert.assertNull(publisher.getStats(topic));
    publisher.publish(topic, msg);
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(),
        1, "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(),
        1, "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(),
        0, "unhandledexception count");
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);
    Assert.assertEquals(publisher.getStatsExposer(topic).getContexts().get(
        TopicStatsExposer.STATS_TYPE_CONTEXT_NAME),
        TopicStatsExposer.STATS_TYPE);
    Assert.assertEquals(publisher.getStatsExposer(topic).getContexts().get(
        TopicStatsExposer.TOPIC_CONTEXT_NAME), topic);
  }
  
}
