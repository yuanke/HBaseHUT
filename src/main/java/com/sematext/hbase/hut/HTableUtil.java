/*
 * Copyright (c) Sematext International
 * All Rights Reserved
 * <p/>
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 */
package com.sematext.hbase.hut;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Provides utility methods on top of {@link org.apache.hadoop.hbase.client.HTable}
 */
public final class HTableUtil {
  private static final int DELETES_BUFFER_MAX_SIZE = 10000; // TODO: Make configurable by user ?

  private HTableUtil() {}

  public static void deleteRange(HTable hTable, byte[] firstInclusive, byte[] lastInclusive, byte[] processingResultToLeave) throws IOException {
    Scan scan = new Scan(firstInclusive, lastInclusive);
    ResultScanner toDeleteScanner = hTable.getScanner(scan);
    Result toDelete = toDeleteScanner.next();
    // Huge number of deletes can eat up all memory, hence keeping buffer
    ArrayList<Delete> listToDelete = new ArrayList<Delete>(DELETES_BUFFER_MAX_SIZE);
    while (toDelete != null) {
      if (!Bytes.equals(processingResultToLeave, toDelete.getRow())) {
        listToDelete.add(new Delete(toDelete.getRow()));
        if (listToDelete.size() >= DELETES_BUFFER_MAX_SIZE) {
          hTable.delete(listToDelete);
          listToDelete.clear();
        }
      }
      toDelete = toDeleteScanner.next();
    }
    // it is omitted during scan, since stopRow specified for scan means "non-inclusive", so adding here
    if (!Bytes.equals(processingResultToLeave, lastInclusive)) {
      listToDelete.add(new Delete(lastInclusive));
    }

    if (listToDelete.size() > 0) {
      hTable.delete(listToDelete);
    }
  }
}
