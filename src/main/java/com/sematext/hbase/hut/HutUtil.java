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

import java.io.IOException;

/**
 * Provides utility methods for HBaseHUT operations on HTable.
 * These operations cannot be easily integrated within current HBase API (pre Coprocessors release).
 * NOTE: This class made as static util and not as wrapper for ease of use of HTable instances when HTablePool is used 
 */
public final class HutUtil {
  private HutUtil() {}

  /**
   * Performs deletion.
   * The same as {@link org.apache.hadoop.hbase.client.HTable#delete(Delete)} operation, i.e.
   * <tt>hTable.delete(delete)</tt> should be substituted to <tt>HutUtil.delete(hTable, delete)</tt>
   *
   * @param hTable table to perform deletion in
   * @param delete record to delete
   * @throws java.io.IOException when underlying HTable operations throw exception
   */
  public static void delete(HTable hTable, Delete delete) throws IOException {
    delete(hTable, delete.getRow());
  }

  /**
   * Performs deletion.
   * See also {@link #delete(org.apache.hadoop.hbase.client.HTable, org.apache.hadoop.hbase.client.Delete)}.
   * @param hTable table to perform deletion in
   * @param row row of record to delete
   * @throws java.io.IOException when underlying HTable operations throw exception
   */
  public static void delete(HTable hTable, byte[] row) throws IOException {
    byte[] stopRow = HutRowKeyUtil.createNewKey(row, Long.MAX_VALUE);
    HTableUtil.deleteRange(hTable, row, stopRow);
  }
}
