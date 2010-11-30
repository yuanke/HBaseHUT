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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * HBaseHUT {@link org.apache.hadoop.hbase.client.ResultScanner} implementation.
 * Use it when scanning records written as {@link com.sematext.hbase.hut.HutPut}s
 */
public class HutResultScanner implements ResultScanner {
  private final ResultScanner resultScanner;
  private Result nonConsumed = null;
  private final UpdateProcessor updateProcessor;
  private boolean storeProcessedUpdates;
  private final HTable hTable;
  // Can be converted to local variable, but we want to reuse iterable instance
  private IterableRecords iterableRecords = new IterableRecords();
  // Can be converted to local variable, but we want to reuse processingResult instance
  private UpdateProcessingResultImpl processingResult = new UpdateProcessingResultImpl();

  public HutResultScanner(ResultScanner resultScanner, UpdateProcessor updateProcessor) {
    this(resultScanner, updateProcessor, null, false);
  }

  public HutResultScanner(ResultScanner resultScanner, UpdateProcessor updateProcessor, HTable hTable, boolean storeProcessedUpdates) {
    if (storeProcessedUpdates && hTable == null) {
      throw new IllegalArgumentException("HTable is null, but access to it required for storing processed updates back.");
    }
    this.resultScanner = resultScanner;
    this.updateProcessor = updateProcessor;
    this.storeProcessedUpdates = storeProcessedUpdates;
    this.hTable = hTable;
  }

  @Override
  public Result next() throws IOException {
    Result firstResult = nonConsumed != null ? nonConsumed : resultScanner.next();
    nonConsumed = null;

    if (firstResult == null) {
      return firstResult;
    }

    Result nextToFirstResult = resultScanner.next();
    if (nextToFirstResult == null) {
      return firstResult;
    }

    if (!HutRowKeyUtil.sameOriginalKeys(firstResult.getRow(), nextToFirstResult.getRow())) {  // nothing to process
      nonConsumed = nextToFirstResult;
      return firstResult;
    }

    iterableRecords.init(firstResult, nextToFirstResult);
    processingResult.init(firstResult.getRow(), firstResult.raw());
    updateProcessor.process(iterableRecords, processingResult);
    // TODO: allow client code specify skipping this?
    // Reading records of this group to the end
    while (iterableRecords.iterator.hasNext()) {
      iterableRecords.iterator.next();
    }

    Result result = processingResult.getResult();
    if (storeProcessedUpdates) {
      storeProcessedUpdates(result, iterableRecords.iterator.lastRead);
    }

    return result;
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    // Identical to HTable.ClientScanner implementation
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);

  }
  
  @Override
  public Iterator<Result> iterator() {
    // Identical to HTable.ClientScanner implementation
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      public boolean hasNext() {
        if (next == null) {
          try {
            next = HutResultScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void close() {
    resultScanner.close();
  }

  private static class UpdateProcessingResultImpl implements UpdateProcessingResult {
    private KeyValue[] kvs;
    private byte[] row;

    public void init(byte[] row, KeyValue[] kvs) {
      this.kvs = kvs;
      this.row = row;
    }

    // TODO: revise this method implementation, it can be done better (and/or more efficient)
    @Override
    public void add(byte[] colFam, byte[] qualifier, byte[] value) {
      // TODO: Defer merging to getResult method?
      boolean found = false;
      for (int i = 0; i < kvs.length; i++) {
        KeyValue kv = kvs[i];
        // TODO: make use of timestamp value when comparing?
        if(Bytes.equals(colFam, kv.getFamily()) && Bytes.equals(qualifier, kv.getQualifier())) {
          KeyValue merged = new KeyValue(row, colFam, qualifier, kv.getTimestamp(), value);
          kvs[i] = merged;
          found = true;
          break; // TODO: do we need to update here other KeyValues (or just most recent one)?
        }
      }
      if (!found) {
        kvs = Arrays.copyOf(kvs, kvs.length + 1); // TODO: looks like not vey optimal
        kvs[kvs.length - 1] = new KeyValue(row, colFam, qualifier, value);
      }
    }

    // TODO: revise this method implementation, it can be done better (and/or more efficient)
    @Override
    public void delete(byte[] colFam, byte[] qualifier) {
      // TODO: Defer merging to getResult method?
      for (int i = 0; i < kvs.length; i++) {
        KeyValue kv = kvs[i];
        // TODO: make use of timestamp value when comparing?
        if(Bytes.equals(colFam, kv.getFamily()) && Bytes.equals(qualifier, kv.getQualifier())) {
          // TODO: looks like not vey optimal
          KeyValue[] newKvs = new KeyValue[kvs.length - 1];
          System.arraycopy(kvs, 0, newKvs, 0, i);
          System.arraycopy(kvs, i, newKvs, i + 1, newKvs.length - i);
          kvs = newKvs;
          break; // TODO: do we need to update here other KeyValues (or just most recent one)?
        }
      }
    }

    public Result getResult() {
      return new Result(kvs);
    }
  }

  private class IterableRecords implements Iterable<Result> {
    // reusing iterator instance
    private IteratorImpl iterator = new IteratorImpl();

    // Accepts at least two records: no point in starting processing unless we have more than one
    public void init(Result first, Result nextToFirst) {
      this.iterator.firstRecordKey = first.getRow();
      this.iterator.next = null;
      this.iterator.exhausted = false;
      this.iterator.lastRead = null;
      this.iterator.doFirstHasNext(first, nextToFirst);
    }

    private class IteratorImpl implements Iterator<Result> {
      private byte[] firstRecordKey;
      private Result next; // next record prepared (and processed) for fetching
      private Result lastRead;
      boolean exhausted;

      private void doFirstHasNext(Result first, Result nextToFirst) {
        try {
          next = first;
          // we start directly with the last steps of hasNext as all other conditions were checked in resultScanner's next()
          // Skipping those which were processed but haven't deleted yet (very small chance to face this) -
          // skipping records that are stored before processing result
          while (HutRowKeyUtil.sameRecords(first.getRow(), nextToFirst.getRow())) {
            next = nextToFirst;
            nextToFirst = resultScanner.next();
            if (nextToFirst == null) {
              return;
            }
          }

          nonConsumed = nextToFirst;

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      @Override
      public boolean hasNext() {
        if (exhausted) {
          return false;
        }
        if (next != null) {
          return true;
        }
        try {
          Result nextCandidate = nonConsumed != null ? nonConsumed : resultScanner.next();
          if (nextCandidate == null) {
            exhausted = true;
            return false;
          }

          // TODO: nextCandidate.getRow() reads all fields (HBase internal implementation), but we actually may need only row here
          boolean sameOriginalKeys = HutRowKeyUtil.sameOriginalKeys(firstRecordKey, nextCandidate.getRow());

          if (!sameOriginalKeys) {
            nonConsumed = nextCandidate;
            exhausted = true;
            return false;
          }

          // Skipping those which where already processed but hasn't been deleted yet to keep results consistent.
          // There's tiny chance for that: may occur writing processed interval's data
          // and deleting all processed records in the interval is not atomic.
          // Also allows not to delete records at all during compaction (in case we want and able to process them more than once).
          while (lastRead != null && sameOriginalKeys && !HutRowKeyUtil.isAfter(nextCandidate.getRow(), lastRead.getRow())) {
            nextCandidate = resultScanner.next();
            if (nextCandidate == null) {
              exhausted = true;
              return false;
            }
            sameOriginalKeys = HutRowKeyUtil.sameOriginalKeys(firstRecordKey, nextCandidate.getRow());
          }

          if (!sameOriginalKeys) {
            nonConsumed = nextCandidate;
            exhausted = true;
            return false;
          }

          next = nextCandidate;
          nonConsumed = null;

          // Skipping those which were processed but haven't deleted yet (very small chance to face this)
          // skipping records that are stored before processing result
          Result afterNextCandidate = resultScanner.next();
          if (afterNextCandidate == null) {
            return true;
          }

          while (HutRowKeyUtil.sameRecords(nextCandidate.getRow(), afterNextCandidate.getRow())) {
            next = afterNextCandidate;
            afterNextCandidate = resultScanner.next();
            if (afterNextCandidate == null) {
              return true;
            }
          }

          nonConsumed = afterNextCandidate;

          return true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        lastRead = next;
        next = null;
        return lastRead;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public Iterator<Result> iterator() {
      return iterator;
    }
  }

  // Can be converted to local variable, but we want to reuse list instance
  private ArrayList<Delete> listToDelete = new ArrayList<Delete>();

  private void storeProcessedUpdates(Result first, Result last) throws IOException {
    byte[] firstRow = first.getRow();
    byte[] row = Arrays.copyOf(firstRow, firstRow.length);
    HutRowKeyUtil.setIntervalEnd(row, last.getRow()); // can row here remain the same?
    Put put = new Put(row);
    for (KeyValue kv : first.raw()) {
      // overriding row TODO: do we need to invalidate row cache of kv here?
      overrideRow(kv, row);
      put.add(kv);
    }
    hTable.put(put);

    deleteProcessedRecords(first.getRow(), last.getRow(), row);
  }

  // NOTE: this works only when rows has the same length, and doesn't invalidate row cache
  static void overrideRow(KeyValue kv, byte[] row) {
    System.arraycopy(row, 0, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
  }

  void deleteProcessedRecords(byte[] firstInclusive, byte[] lastInclusive, byte[] processingResultToLeave) throws IOException {
    Scan scan = new Scan(firstInclusive, lastInclusive);
    ResultScanner toDeleteScanner = hTable.getScanner(scan);
    // skipping first: it is a result of processing, should be left
    Result toDelete = toDeleteScanner.next();
    listToDelete.clear();
    while (toDelete != null) {
      if (!Bytes.equals(processingResultToLeave, toDelete.getRow())) {
        listToDelete.add(new Delete(toDelete.getRow()));
      }
      toDelete = toDeleteScanner.next();
    }
    // it is omitted during scan, since stopRow specified for scan means "non-inclusive", so adding here
    if (!Bytes.equals(processingResultToLeave, lastInclusive)) {
      listToDelete.add(new Delete(lastInclusive));
    }

    hTable.delete(listToDelete);
  }
}
