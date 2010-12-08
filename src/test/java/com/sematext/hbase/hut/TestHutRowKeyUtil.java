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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class TestHutRowKeyUtil {
  @Test
  public void test() {
    byte[] original = Bytes.toBytes("original");
    long startTime = System.currentTimeMillis();
    byte[] key1 = HutRowKeyUtil.createNewKey(original, startTime);
    byte[] key2 = HutRowKeyUtil.createNewKey(original, startTime + 2);
    byte[] key3 = HutRowKeyUtil.createNewKey(original, startTime + 3);
    byte[] key4 = HutRowKeyUtil.createNewKey(original, startTime + 4);
    byte[] key5 = HutRowKeyUtil.createNewKey(original, startTime + 5);

    Assert.assertTrue(HutRowKeyUtil.sameOriginalKeys(key1, key2));
    Assert.assertTrue(HutRowKeyUtil.sameOriginalKeys(key2, key3));
    Assert.assertTrue(HutRowKeyUtil.sameOriginalKeys(key1, key3));

    // set key2 to hover interval from key2 to key4
    byte[] key2Copy = Arrays.copyOf(key2, key2.length);
    HutRowKeyUtil.setIntervalEnd(key2, key4);
    Assert.assertArrayEquals(key2Copy, HutRowKeyUtil.getStartRowOfInterval(key2));
    Assert.assertArrayEquals(key4, HutRowKeyUtil.getEndRowOfInterval(key2));
    Assert.assertTrue(HutRowKeyUtil.sameRecords(key2Copy, key2));
    Assert.assertFalse(HutRowKeyUtil.isAfter(key3, key2));
    Assert.assertFalse(HutRowKeyUtil.isAfter(key4, key2));
    Assert.assertTrue(HutRowKeyUtil.isAfter(key5, key2));

    // set key1 to hover interval from key1 to key2 (and hence, from key1 to key4)
    HutRowKeyUtil.setIntervalEnd(key1, key2);
    Assert.assertFalse(HutRowKeyUtil.isAfter(key3, key1));
    Assert.assertFalse(HutRowKeyUtil.isAfter(key4, key1));
    Assert.assertTrue(HutRowKeyUtil.isAfter(key5, key1));
  }
}
