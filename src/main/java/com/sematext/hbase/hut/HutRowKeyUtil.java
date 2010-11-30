/**
 * Copyright 2010 Sematext International
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sematext.hbase.hut;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides common method for key manipulation.
 *
 * HBaseRT adjusted key adds two long values to the end, comparing to the original key: creationTime and intervalEnd.
 * First long value (creationTime) is set when HBase record (<tt>Put</tt>) is created.
 * If second long value (intervalEnd) is > 0 then the record defined by the key represents processed (aka merged) data for
 * the whole interval of records with creationTime between these two values.
 */
public final class HutRowKeyUtil {
  private static final byte[] NOT_SET_MARK = Bytes.toBytes(0L);

  private HutRowKeyUtil() {}

  public static byte[] getOriginalKey(byte[] hrtRowKey) {
    return Bytes.head(hrtRowKey, hrtRowKey.length - Bytes.SIZEOF_LONG * 2);
  }

  static boolean sameOriginalKeys(byte[] hrtRowKey1, byte[] hrtRowKey2) {
    return 0 == Bytes.compareTo(hrtRowKey1, 0, hrtRowKey1.length - Bytes.SIZEOF_LONG * 2,
                                hrtRowKey2, 0, hrtRowKey2.length - Bytes.SIZEOF_LONG * 2);
  }

  // TODO: rename it or explain
  static boolean sameRecords(byte[] hrtRowKey1, byte[] hrtRowKey2) {
    return 0 == Bytes.compareTo(hrtRowKey1, 0, hrtRowKey1.length - Bytes.SIZEOF_LONG,
                                hrtRowKey2, 0, hrtRowKey2.length - Bytes.SIZEOF_LONG);
  }

  /**
   * Is first key goes after the interval defined by the second key
   * @param hrtRowKeyToCompare first key
   * @param hrtRowKey second key
   * @return true if second key goes after the first key
   */
  static boolean isAfter(byte[] hrtRowKeyToCompare, byte[] hrtRowKey) {
    // comparing creationTime of second key with intervalEnd of the first key
    return Bytes.compareTo(hrtRowKeyToCompare, hrtRowKeyToCompare.length - Bytes.SIZEOF_LONG * 2, Bytes.SIZEOF_LONG,
                                hrtRowKey, hrtRowKey.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG) > 0;
  }

  static byte[] createNewKey(byte[] hrtRowKey, long creationTime) {
    return Bytes.add(hrtRowKey, Bytes.toBytes(creationTime), NOT_SET_MARK);
  }

  static void setIntervalEnd(byte[] hrtRowKey, byte[] lastRowKeyInInterval) {
    boolean isIntervalEndSetForLastRowKey =
            Bytes.compareTo(lastRowKeyInInterval, lastRowKeyInInterval.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG,
                    NOT_SET_MARK, 0, NOT_SET_MARK.length) != 0;
    if (isIntervalEndSetForLastRowKey) {
      // setting hrtRowKey.intervalEnd = lastRowKeyInInterval.intervalEnd
      System.arraycopy(lastRowKeyInInterval, lastRowKeyInInterval.length - Bytes.SIZEOF_LONG,
                       hrtRowKey, hrtRowKey.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
    } else {
      // setting hrtRowKey.intervalEnd = lastRowKeyInInterval.creationTime
      System.arraycopy(lastRowKeyInInterval, lastRowKeyInInterval.length - Bytes.SIZEOF_LONG * 2,
                       hrtRowKey, hrtRowKey.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
    }
  }
}
