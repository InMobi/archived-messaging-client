package com.inmobi.databus.files;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCollectorFile {

  @Test
  public void testCollectorFile() throws ParseException {
  	Calendar now = Calendar.getInstance();
    String stream1 = "a";
    String stream2 = "b";
    Date date = now.getTime();
    String dateStr = CollectorFile.fileFormat.get().format(date);
    CollectorFile cf1 = new CollectorFile(stream1, date, 0);
    String cf1Str = new String(stream1 + "-" + dateStr + "_00000");
    Assert.assertEquals(cf1.toString(), cf1Str);
    Assert.assertEquals(cf1, CollectorFile.create(cf1Str));
    Assert.assertEquals(cf1.getStreamName(), stream1);
    Assert.assertEquals(cf1.getTimestamp(),
        CollectorFile.fileFormat.get().parse(dateStr));
    Assert.assertEquals(cf1.getId(), 0);

    CollectorFile cf2 = new CollectorFile(stream1, date, 1);
    Assert.assertTrue(cf1.hashCode()< cf2.hashCode());
    Assert.assertEquals(cf2.compareTo(cf1), 1);
    now.add(Calendar.MINUTE, 1);
    cf1 = new CollectorFile(stream1, now.getTime(), 1);
    Assert.assertEquals(cf1.compareTo(cf2), 1);
    cf1 = new CollectorFile(stream2, date, 0);
    Assert.assertEquals(cf1.compareTo(cf2), 1);
    
    Throwable th = null;
    try {
      cf1 = CollectorFile.create("invalid");
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
    
    th = null;
    try {
      cf1 = CollectorFile.create(stream1 + "-" + dateStr);
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    th = null;
    try {
      cf1 = CollectorFile.create(stream1 + "-" + "invaliddate_00000");
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    th = null;
    try {
      cf1 = CollectorFile.create(stream1 + "-" + dateStr + "_invalidid");
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }
}
