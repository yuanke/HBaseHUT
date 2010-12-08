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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Perform processing updates by running MapReduce job, and hence utilizes data locality during work (to some extend).
 */
public final class UpdatesProcessingMrJob {
  private UpdatesProcessingMrJob() {}

  public static class UpdatesProcessingMapper extends TableMapper<ImmutableBytesWritable, Put> {
    public static final String HUT_MR_BUFFER_SIZE_ATTR = "hut.mr.buffer.size";
    public static final String HUT_PROCESSOR_CLASS_ATTR = "hut.processor.class";
    private static final Log LOG = LogFactory.getLog(UpdatesProcessingMapper.class);

    private int bufferMaxSize = 1000; // default value
    private DetachedHutResultScanner resultScanner;
    private Result[] buff;
    private int filledInBuff;
    private int nextInBuff;
    private Thread processingThread;
    private volatile boolean failed;
    ReentrantLock lock = new ReentrantLock(false);
    private Condition waitingOnEmptyBuffer = lock.newCondition();
    Condition waitingOnAllBufferConsumed = lock.newCondition();
    volatile boolean mapTaskFinished;

    // No need to make buff operations thread-safe as buffer is going to accessed from single thread at a time
    private boolean isBufferExhausted() {
      return filledInBuff - nextInBuff <= 0;
    }

    // No need to make buff operations thread-safe as buffer is going to accessed from single thread at a time
    private boolean isBufferFull() {
      boolean full = filledInBuff >= bufferMaxSize;
      if (full) {
        System.out.println("buffer is full");
      }
      return full;
    }

    // No need to make buff operations thread-safe as buffer is going to accessed from single thread at a time
    private Result popFromBuff() {
      Result res = buff[nextInBuff++];
      System.out.println("left: " + (filledInBuff - nextInBuff));
      return res;
    }

    // No need to make buff operations thread-safe as buffer is going to accessed from single thread at a time
    private void pushToBuff(Result result) {
      if (isBufferFull()) {
        filledInBuff = 0;
        nextInBuff = 0;
      }
      buff[filledInBuff++] = result;
      System.out.println("in buff now: " + (filledInBuff - nextInBuff));
    }

    /**
     * Detached from HTable and ResultScanner scanner that is being fed with {@link org.apache.hadoop.hbase.client.Result} items.
     * Differs from HutResultScanner which uses normal HBase ResultScanner to fetch the data.
     */
    class DetachedHutResultScanner extends HutResultScanner {
      private Put processedUpdatesToStore = null;

      public DetachedHutResultScanner(UpdateProcessor updateProcessor) {
        super(null, updateProcessor, null, true);
      }

      @Override
      void store(Put put) throws IOException {
        processedUpdatesToStore = put;
      }

      @Override
      void deleteProcessedRecords(byte[] firstInclusive, byte[] lastInclusive, byte[] processingResultToLeave) throws IOException {
        // DO NOTHING
      }

      @Override
      void verifyInitParams(ResultScanner resultScanner, UpdateProcessor updateProcessor, HTable hTable, boolean storeProcessedUpdates) {
        if (updateProcessor == null) {
          throw new IllegalArgumentException("UpdateProcessor should NOT be null.");
        }
        // since this is "detached" scanner, ResultScanner and/or HTable can be null
      }

      @Override
      public Result next() throws IOException {
        processedUpdatesToStore = null;

        return super.next();
      }

      Result fetchNext() throws IOException {
        // trying to fetch data from nextItemsToFetch buffer
        if (isBufferExhausted()) {
          if (mapTaskFinished) { // no more items expected
            return null;
          }
          lock.lock();
          try {
            waitingOnAllBufferConsumed.signal();
            while (isBufferExhausted()) {
              try {
                waitingOnEmptyBuffer.await();
              } catch (InterruptedException e) {
                // DO NOTHING
              }
            }
          } finally {
            lock.unlock();
          }
        }

        return popFromBuff();
      }

      public Put getProcessedUpdatesToStore() {
        return processedUpdatesToStore;
      }
    }

    /**
     * Pass the key, value to reduce.
     *
     * @param key  The current key.
     * @param value  The current value.
     * @param context  The current context.
     * @throws IOException When writing the record fails.
     * @throws InterruptedException When the job is aborted.
     */
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      if (isBufferFull()) {
        try {
          lock.lock();
          waitingOnEmptyBuffer.signal();
          while (!isBufferExhausted()) {
            waitingOnAllBufferConsumed.await();
          }
        } finally {
          lock.unlock();
        }
      }

      pushToBuff(value);
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      super.setup(context);

      String updatesProcessorClass =  context.getConfiguration().get(HUT_PROCESSOR_CLASS_ATTR);
      if (updatesProcessorClass == null) {
        throw new IllegalStateException("hut.processor.class missed in the configuration");
      }
      UpdateProcessor updateProcessor = createInstance(updatesProcessorClass, UpdateProcessor.class);
      resultScanner = new DetachedHutResultScanner(updateProcessor);

      String bufferSizeValue =  context.getConfiguration().get(HUT_MR_BUFFER_SIZE_ATTR);
      if (bufferSizeValue == null) {
        LOG.info(HUT_MR_BUFFER_SIZE_ATTR + " is missed in the configuration, using default value: " + bufferMaxSize);
      } else {
        bufferMaxSize = Integer.valueOf(bufferSizeValue);
      }

      buff = new Result[bufferMaxSize];
      filledInBuff = 0;
      nextInBuff = 0;
      mapTaskFinished = false;
      failed = false;

      processingThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Result res = resultScanner.next();
            while (res != null) {
              Put processingResultToStore = resultScanner.getProcessedUpdatesToStore();
              if (processingResultToStore != null) {
                context.write(new ImmutableBytesWritable(processingResultToStore.getRow()), processingResultToStore);
              }
              res = resultScanner.next();
            }
          } catch (IOException e) {
            LOG.error(e);
            failed = true; // marking job as failed
          } catch (InterruptedException e) {
            LOG.error(e);
            failed = true; // marking job as failed
          }
        }
      });
      processingThread.start();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (!isBufferExhausted()) { // passing non-consumed items
        try {
          lock.lock();
          mapTaskFinished = true;
          waitingOnEmptyBuffer.signal();
        } finally {
          lock.unlock();
        }
      }
      processingThread.join();

      if (failed) {
        throw new RuntimeException("Job was marked as failed");
      }
      
      super.cleanup(context);
    }

    @SuppressWarnings ({"unchecked", "unused"})
    private static <T>T createInstance(String className, Class<T> clazz) {
      try {
        Class c = Class.forName(className);
        return (T) c.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException("Could not create class instance.", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Could not create class instance.", e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not create class instance.", e);
      }
    }
  }

  public static class UpdatesProcessingReducer
  extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
    public static final String HTABLE_ATTR_NAME = "htable.name";
    private HTable hTable;

    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(UpdatesProcessingReducer.class);

    /**
     * Writes each given record, consisting of the row key and the given values,
     * to the configured {@link org.apache.hadoop.mapreduce.OutputFormat}. It is emitting the row key and each
     * {@link org.apache.hadoop.hbase.client.Put Put} or
     * {@link org.apache.hadoop.hbase.client.Delete Delete} as separate pairs.
     *
     * @param key  The current row key.
     * @param values  The {@link org.apache.hadoop.hbase.client.Put Put} or
     *   {@link org.apache.hadoop.hbase.client.Delete Delete} list for the given
     *   row.
     * @param context  The context of the reduce.
     * @throws IOException When writing the record fails.
     * @throws InterruptedException When the job gets interrupted.
     */
    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> values,
        Context context) throws IOException, InterruptedException {
      for(Put updatesProcessingResult : values) {
        byte[] row = key.get();
        context.write(key, updatesProcessingResult);
        // TODO: replace htable.delete with writing to context?
        HTableUtil.deleteRange(hTable,
                HutRowKeyUtil.getStartRowOfInterval(row), HutRowKeyUtil.getEndRowOfInterval(row), row);
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      // TODO: add validation of configuration attributes
      hTable = new HTable(context.getConfiguration().get(HTABLE_ATTR_NAME));
    }
  }

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table  The table name.
   * @param scan  The scan with the columns to scan.
   * @param updateProcessorClass update processor implemenation
   * @param job  The job configuration.
   * @throws java.io.IOException When setting up the job fails.
   */
  @SuppressWarnings("unchecked")
  public static void initJob(String table, Scan scan, Class<? extends UpdateProcessor> updateProcessorClass, Job job)
          throws IOException {
    TableMapReduceUtil.initTableMapperJob(table, scan, UpdatesProcessingMapper.class,
      ImmutableBytesWritable.class, Put.class, job);
    TableMapReduceUtil.initTableReducerJob(table, UpdatesProcessingReducer.class, job, HRegionPartitioner.class);
    job.setJarByClass(UpdatesProcessingMrJob.class);
    job.getConfiguration().set(UpdatesProcessingReducer.HTABLE_ATTR_NAME, table);
    job.getConfiguration().set(UpdatesProcessingMapper.HUT_PROCESSOR_CLASS_ATTR, updateProcessorClass.getName());
  }
}
