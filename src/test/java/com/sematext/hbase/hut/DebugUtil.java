/*
 * Copyright (c) Sematext International
 * All Rights Reserved
 * <p/>
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 */
package com.sematext.hbase.hut;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 */
public final class DebugUtil {
  private static final String C_DEL = "\t\t";
  private static final String CF_DEL = ":";
  private static final String VAL_DEL = "=";

  public DebugUtil() {}

  public static String getContent(HTable t) throws IOException {
    ResultScanner rs = t.getScanner(new Scan());
    Result next = rs.next();
    int readCount = 0;
    StringBuilder contentText = new StringBuilder();
    while (next != null && readCount < 1000) {
      contentText.append(getHutRowKeyAsText(next.getRow()));
      for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cf : next.getMap().entrySet()) {
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> c : cf.getValue().entrySet()) {
          byte[] value = c.getValue().values().iterator().next();
          contentText.append(C_DEL);
          contentText.append(Bytes.toString(cf.getKey())).append(CF_DEL)
                  .append(Bytes.toString(c.getKey())).append(VAL_DEL)
                  .append(getText(value));
        }
      }
      contentText.append("\n");
      next = rs.next();
      readCount++;
    }

    return contentText.toString();
  }

  private static String getHutRowKeyAsText(byte[] row) {
    return Bytes.toString(HutRowKeyUtil.getOriginalKey(row)) + "-" +
            Bytes.toLong(Bytes.head(Bytes.tail(row, 2 * Bytes.SIZEOF_LONG), Bytes.SIZEOF_LONG)) + "-" +
            Bytes.toLong(Bytes.tail(row, Bytes.SIZEOF_LONG));
  }

  private static String getText(byte[] data) {
    try {
      if (data.length == Bytes.SIZEOF_INT) {
        return String.valueOf(Bytes.toInt(data));
      } else if (data.length == Bytes.SIZEOF_LONG) {
        return String.valueOf(Bytes.toLong(data));
      } else {
        return Bytes.toString(data);
      }
    } catch (IllegalArgumentException ex) {
      return Bytes.toString(data);
    }
  }
}
