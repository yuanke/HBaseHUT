/*
 * Copyright (c) Sematext International
 * All Rights Reserved
 * <p/>
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 */
package com.sematext.hbase.hut;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class TestHutResultScanner {
  @Test
  public void testOverrideRow() {
    byte[] row = Bytes.toBytes("some-key");
    KeyValue kv = new KeyValue(row, 55L);
    // NOTE: keys should have sme length. Also getRow shouldn't be called on this keyValue before: cache won't be invalidated
    byte[] row2 = Bytes.toBytes("other!!!");
    HutResultScanner.overrideRow(kv, row2);
    Assert.assertTrue(Bytes.equals(row2, kv.getRow()));
  }
}
