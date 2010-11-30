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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

/**
 * HBaseHUT {@link org.apache.hadoop.hbase.client.Put} implementation.
 * Use it when you want to use advantage of HBaseHUT updates processing logic.
 */
public class HutPut extends Put {
  public HutPut(byte[] row) {
    super(adjustRow(row));
  }

  public HutPut(byte[] row, RowLock rowLock) {
    super(adjustRow(row), rowLock);
  }

  public HutPut(Put putToCopy) {
    super(putToCopy);
  }

  private static byte[] adjustRow(byte[] row) {
    return HutRowKeyUtil.createNewKey(row, System.currentTimeMillis());
  }
}
