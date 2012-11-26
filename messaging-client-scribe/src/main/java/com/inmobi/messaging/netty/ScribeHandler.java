package com.inmobi.messaging.netty;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.netty.ScribeTopicPublisher.ChannelSetter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import scribe.thrift.ResultCode;

public class ScribeHandler extends SimpleChannelHandler {
  private static final Log LOG = LogFactory.getLog(ScribeHandler.class);

  private final TimingAccumulator stats;
  private final ChannelSetter channelSetter;
  private volatile long connectRequestTime = 0;
  private long backoffSeconds;
  private Timer timer;
  private boolean connectionInited = false;
  private volatile boolean reconnectInprogress = false;
  private final Semaphore lock = new Semaphore(1);
  private final ScribeTopicPublisher thisPublisher;
  private boolean exceptionDuringConnect = false;
  private boolean closed = false;

  public ScribeHandler(TimingAccumulator stats, ChannelSetter channelSetter,
      int backoffSeconds, Timer timer, ScribeTopicPublisher publisher) {
    this.stats = stats;
    this.channelSetter = channelSetter;
    this.backoffSeconds = backoffSeconds;
    this.timer = timer;
    thisPublisher = publisher;
  }

  void setInited() {
    connectionInited = true;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {

    ResultCode success;
    ChannelBuffer buf = (ChannelBuffer) e.getMessage();
    TMemoryInputTransport trans = new TMemoryInputTransport(buf.array());
    TBinaryProtocol proto = new TBinaryProtocol(trans);
    TMessage msg = proto.readMessageBegin();
    if (msg.type == TMessageType.EXCEPTION) {
      proto.readMessageEnd();
    }
    TField field;
    proto.readStructBegin();
    while (true) {
      field = proto.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      switch (field.id) {
      case 0: // SUCCESS
        if (field.type == TType.I32) {
          success = ResultCode.findByValue(proto.readI32());
          thisPublisher.ack(success);
        } else {
          TProtocolUtil.skip(proto, field.type);
        }
        break;
      default:
        TProtocolUtil.skip(proto, field.type);
      }
      proto.readFieldEnd();
    }
    proto.readStructEnd();
    proto.readMessageEnd();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    Throwable cause = e.getCause();

    LOG.warn("Exception caught:", cause);
    stats.accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);

    if (cause instanceof ReadTimeoutException) {
      if (!thisPublisher.isAckQueueEmpty()) {
        LOG.info("Not reconnecting for ReadTimeout, as ackqueue is not empty");
        return;
      }
    }

    if (channelSetter.getCurrentChannel() != null && 
        ctx.getChannel().getId() == channelSetter.getCurrentChannel().getId()) {
      scheduleReconnect();
    } else {
      LOG.info("Ignoring exception " + cause + " because it was on" + 
          " channel" + ctx.getChannel().getId());
    }
  }

  void scheduleReconnect() {
    if (!closed && connectionInited) {
      if (!reconnectInprogress || exceptionDuringConnect) {
        /*
         * Ensure you are the only one mucking with connections If you find someone
         * else is doing so, then you don't get a turn We trust this other person to
         * do the needful
         */
        if (lock.tryAcquire()) {
          long currentTime = System.currentTimeMillis();
          // Check how long it has been since we reconnected
          try {
            if ((currentTime - connectRequestTime) / 1000 > backoffSeconds
                && !reconnectInprogress) {
              prepareReconnect();
              reconnectInprogress = true;
              connectRequestTime = currentTime;
              timer.newTimeout(new TimerTask() {
                
                public void run(Timeout timeout) throws Exception {
                  LOG.info("Connecting now");
                  try {
                    channelSetter.connect();
                  } catch (Exception e) {
                    LOG.warn("got exception during connect");
                    setExceptionDuringConnect();
                    return;
                  }
                  stats.accumulateReconnections();
                  reconnectInprogress = false;
                  thisPublisher.doneReconnect();
                }
              }, backoffSeconds, TimeUnit.SECONDS);
            }
          } finally {
            lock.release();
          }
        }

      } else {
        LOG.info("Not connecting now, because connection is already in " +
            "progress");
      }
    } else {
      LOG.info("Not connecting, because connection is not intialized or is" +
          " closed");
    }
  }

  public void channelDisconnected(ChannelHandlerContext ctx,
      ChannelStateEvent e) {
    if (channelSetter.getCurrentChannel() != null && 
        ctx.getChannel().getId() == channelSetter.getCurrentChannel().getId()) {
      LOG.info("Channel disconnected");
      scheduleReconnect();
    }
  }

  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
    if (channelSetter.getCurrentChannel() != null && 
        ctx.getChannel().getId() == channelSetter.getCurrentChannel().getId()) {
      LOG.info("Channel closed");
      scheduleReconnect();
    }
  }

  public void channelUnbound(ChannelHandlerContext ctx, ChannelStateEvent e) {
    if (channelSetter.getCurrentChannel() != null && 
        ctx.getChannel().getId() == channelSetter.getCurrentChannel().getId()) {
      LOG.info("Channel unbound");
      scheduleReconnect();
    }
  }

  private void prepareReconnect() {
    exceptionDuringConnect = false;    
    thisPublisher.prepareReconnect();    
  }

  void setExceptionDuringConnect() {
    exceptionDuringConnect = true;
    reconnectInprogress = false;
    scheduleReconnect();
  }

  void prepareClose() {
    closed = true;
  }
}
