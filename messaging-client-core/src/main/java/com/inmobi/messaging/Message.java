package com.inmobi.messaging;

import java.nio.ByteBuffer;

/**
 *  Message class holding the data.
 *
 */
public final class Message {

  private final ByteBuffer data;

  /**
   * Create new message with {@link ByteBuffer}
   * 
   * @param data The {@link ByteBuffer}
   */
  public Message(ByteBuffer data) {
    this.data = data;
  }

  /**
   * Create new message with byte array
   * 
   * @param data The byte array.
   */
  public Message(byte[] data) {
    this.data = ByteBuffer.wrap(data);
  }

  /**
   * Get the data associated with message.
   * 
   * @return {@link ByteBuffer} holding the data.
   */
  public ByteBuffer getData() {
    return data;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Message other = (Message) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    return true;
  }

  @Override
  public Message clone() {
    Message m = new Message(data.duplicate());
    return m;
  }
}
