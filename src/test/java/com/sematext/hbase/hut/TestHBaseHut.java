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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * General unit-test for the whole concept 
 */
public class TestHBaseHut {
  private HBaseTestingUtility testingUtility;
  public static final byte[] SALE_CF = Bytes.toBytes("sale");

  @Before
  public void before() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
  }

  static class StockSaleUpdateProcessor implements UpdateProcessor {
    @Override
    public void process(Iterable<Result> records, UpdateProcessingResult processingResult) {
      // Processing records
      byte[][] lastPricesBuff = new byte[5][];
      int lastIndex = -1;
      for (Result record : records) {
        for (int i = 0; i < 5; i++) {
          // "lastPrice0" is the most recent one, hence should be added as last
          byte[] price = getPrice(record, "lastPrice" + (4 - i));
          lastIndex = addPrice(lastPricesBuff, price, lastIndex);
        }
      }

      // Writing result
      if (lastIndex == -1) { // nothing to output
        return;
      }
      for (int i = 0; i < 5; i++) {
        // iterating backwards so that "lastPrice0" is set to the most recent one
        byte[] price = lastPricesBuff[(lastIndex + 5 - i) % 5];
        if (price != null) {
          processingResult.add(SALE_CF, Bytes.toBytes("lastPrice" + i), price);
        }
      }
    }

    public int addPrice(byte[][] lastPricesBuff, byte[] price, int lastIndex) {
      if (price == null) {
        return lastIndex;
      }
      lastIndex++;
      if (lastIndex > lastPricesBuff.length - 1) {
        lastIndex = 0;
      }
      lastPricesBuff[lastIndex] = price;
      return lastIndex;
    }

    private byte[] getPrice(Result result, String priceQualifier) {
      return result.getValue(SALE_CF, Bytes.toBytes(priceQualifier));
    }
  }

  @Test
  public void testSimpleScan() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), SALE_CF);
    StockSaleUpdateProcessor processor = new StockSaleUpdateProcessor();

    // Writing data
    byte[] chrysler = Bytes.toBytes("chrysler");
    byte[] ford = Bytes.toBytes("ford");

    verifyLastSales(hTable, processor, chrysler, new int[] {});
    verifyLastSales(hTable, processor, ford, new int[] {});
    recordSale(hTable, chrysler, 90);
    recordSale(hTable, chrysler, 100);
    recordSale(hTable, ford, 18);
    recordSale(hTable, chrysler, 120);
    verifyLastSales(hTable, processor, chrysler, new int[] {120, 100, 90});
    verifyLastSales(hTable, processor, ford, new int[] {18});

    recordSale(hTable, chrysler, 115);
    recordSale(hTable, ford, 22);
    recordSale(hTable, chrysler, 110);
    verifyLastSales(hTable, processor, chrysler, new int[] {110, 115, 120, 100, 90});
    verifyLastSales(hTable, processor, ford, new int[] {22, 18});

    recordSale(hTable, chrysler, 105);
    recordSale(hTable, ford, 24);
    recordSale(hTable, ford, 28);
    verifyLastSales(hTable, processor, chrysler, new int[] {105, 110, 115, 120, 100});
    verifyLastSales(hTable, processor, ford, new int[] {28, 24, 22, 18});

    recordSale(hTable, chrysler, 107);
    recordSale(hTable, ford, 32);
    recordSale(hTable, ford, 40);
    recordSale(hTable, chrysler, 113);
    verifyLastSales(hTable, processor, chrysler, new int[] {113, 107, 105, 110, 115});
    verifyLastSales(hTable, processor, ford, new int[] {40, 32, 28, 24, 22});

    hTable.close();
  }

  @Test
  public void testSimpleScanWithCaching() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), SALE_CF);
    StockSaleUpdateProcessor processor = new StockSaleUpdateProcessor();

    // Writing data
    byte[] chrysler = Bytes.toBytes("chrysler");
    byte[] ford = Bytes.toBytes("ford");
    byte[] toyota = Bytes.toBytes("toyota");

    verifyLastSalesForAllCompanies(hTable, processor, new int[][] {{}, {}});
    recordSale(hTable, chrysler, 90);
    recordSale(hTable, chrysler, 100);
    recordSale(hTable, ford, 18);
    recordSale(hTable, chrysler, 120);
    verifyLastSalesForAllCompanies(hTable, processor, new int[][] {{120, 100, 90}, {18}, {}});

    recordSale(hTable, toyota, 202);
    recordSale(hTable, chrysler, 115);
    recordSale(hTable, toyota, 212);
    recordSale(hTable, toyota, 204);
    recordSale(hTable, ford, 22);
    recordSale(hTable, chrysler, 110);
    verifyLastSalesForAllCompanies(hTable, processor, new int[][] {{110, 115, 120, 100, 90}, {22, 18}, {204, 212, 202}});

    recordSale(hTable, chrysler, 105);
    recordSale(hTable, ford, 24);
    recordSale(hTable, ford, 28);
    verifyLastSalesForAllCompanies(hTable, processor, new int[][] {{105, 110, 115, 120, 100}, {28, 24, 22, 18}, {204, 212, 202}});

    recordSale(hTable, chrysler, 107);
    recordSale(hTable, ford, 32);
    recordSale(hTable, ford, 40);
    recordSale(hTable, toyota, 224);
    recordSale(hTable, chrysler, 113);
    verifyLastSalesForAllCompanies(hTable, processor, new int[][] {{113, 107, 105, 110, 115}, {40, 32, 28, 24, 22}, {224, 204, 212, 202}});

    hTable.close();
  }

  @Test
  public void testStoringProcessedUpdatesDuringScan() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), SALE_CF);
    StockSaleUpdateProcessor processor = new StockSaleUpdateProcessor();

    // Writing data
    byte[] chrysler = Bytes.toBytes("chrysler");
    byte[] ford = Bytes.toBytes("ford");

    recordSale(hTable, chrysler, 90);
    recordSale(hTable, chrysler, 100);
    recordSale(hTable, ford, 18);
    recordSale(hTable, chrysler, 120);
    recordSale(hTable, chrysler, 115);
    recordSale(hTable, ford, 22);
    recordSale(hTable, chrysler, 110);

    verifyLastSalesWithCompation(hTable, processor, chrysler, new int[] {110, 115, 120, 100, 90});
    verifyLastSalesWithCompation(hTable, processor, ford, new int[] {22, 18});
    // after processed updates has been stored, "native" HBase scanner should return processed results too
    verifyLastSalesWithNativeScanner(hTable,  chrysler, new int[] {110, 115, 120, 100, 90});
    verifyLastSalesWithNativeScanner(hTable, ford, new int[] {22, 18});

    recordSale(hTable, chrysler, 105);
    recordSale(hTable, ford, 24);
    recordSale(hTable, ford, 28);
    recordSale(hTable, chrysler, 107);
    recordSale(hTable, ford, 32);
    recordSale(hTable, ford, 40);
    recordSale(hTable, chrysler, 113);

    verifyLastSalesWithCompation(hTable, processor, chrysler, new int[] {113, 107, 105, 110, 115});
    verifyLastSalesWithCompation(hTable, processor, ford, new int[] {40, 32, 28, 24, 22});
    // after processed updates has been stored, "native" HBase scanner should return processed results too
    verifyLastSalesWithNativeScanner(hTable, chrysler, new int[] {113, 107, 105, 110, 115});
    verifyLastSalesWithNativeScanner(hTable, ford, new int[] {40, 32, 28, 24, 22});

    hTable.close();
  }

  @Test
  public void testUpdatesProcessingUtil() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), SALE_CF);
    StockSaleUpdateProcessor processor = new StockSaleUpdateProcessor();

    // Writing data
    byte[] chrysler = Bytes.toBytes("chrysler");
    byte[] ford = Bytes.toBytes("ford");

    recordSale(hTable, chrysler, 90);
    recordSale(hTable, chrysler, 100);
    recordSale(hTable, ford, 18);
    recordSale(hTable, chrysler, 120);
    recordSale(hTable, chrysler, 115);
    recordSale(hTable, ford, 22);
    recordSale(hTable, chrysler, 110);

    UpdatesProcessingUtil.processUpdates(hTable, processor);
    // after processed updates has been stored, "native" HBase scanner should return processed results too
    verifyLastSalesWithNativeScanner(hTable, chrysler, new int[] {110, 115, 120, 100, 90});
    verifyLastSalesWithNativeScanner(hTable, ford, new int[] {22, 18});

    recordSale(hTable, chrysler, 105);
    recordSale(hTable, ford, 24);
    recordSale(hTable, ford, 28);
    recordSale(hTable, chrysler, 107);
    recordSale(hTable, ford, 32);
    recordSale(hTable, ford, 40);
    recordSale(hTable, chrysler, 113);

    UpdatesProcessingUtil.processUpdates(hTable, processor);
    UpdatesProcessingUtil.processUpdates(hTable, processor); // updates processing can be performed when no new data was added
    // after processed updates has been stored, "native" HBase scanner should return processed results too
    verifyLastSalesWithNativeScanner(hTable, chrysler, new int[] {113, 107, 105, 110, 115});
    verifyLastSalesWithNativeScanner(hTable, ford, new int[] {40, 32, 28, 24, 22});

    hTable.close();
  }

  // This test verifies that updates processing can be done on any interval(s) of written data separately,
  // thus updates processing can be performed on per-region basis without breaking things
  @Test
  public void testProcessingUpdatesInSeparateIntervals() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), SALE_CF);
    StockSaleUpdateProcessor processor = new StockSaleUpdateProcessor();

    // Writing data
    byte[] chrysler = Bytes.toBytes("chrysler");
    byte[] ford = Bytes.toBytes("ford");

    recordSale(hTable, chrysler, 90);
    recordSale(hTable, chrysler, 100);
    recordSale(hTable, ford, 18);
    recordSale(hTable, chrysler, 120);
    recordSale(hTable, chrysler, 115);
    recordSale(hTable, ford, 22);
    recordSale(hTable, chrysler, 110);

    performUpdatesProcessingButWithoutDeletionOfProcessedRecords(hTable, processor);
    verifyLastSales(hTable, processor, chrysler, new int[] {110, 115, 120, 100, 90});
    verifyLastSales(hTable, processor, ford, new int[] {22, 18});

    recordSale(hTable, chrysler, 105);
    recordSale(hTable, ford, 24);

    performUpdatesProcessingButWithoutDeletionOfProcessedRecords(hTable, processor);
    verifyLastSales(hTable, processor, chrysler, new int[] {105, 110, 115, 120, 100});
    verifyLastSales(hTable, processor, ford, new int[] {24, 22, 18});

    hTable.close();
  }

  // TODO: add more test-cases for MR job
  @Test
  public void testUpdatesProcessingMrJob() throws IOException, InterruptedException, ClassNotFoundException {
    String tableName = "stock-market";
    HTable hTable = testingUtility.createTable(Bytes.toBytes(tableName), SALE_CF);
    testingUtility.startMiniMapReduceCluster();


    try {
      // Writing data
      byte[] chrysler = Bytes.toBytes("chrysler");
      byte[] ford = Bytes.toBytes("ford");
      byte[] toyota = Bytes.toBytes("toyota");

      for (int i = 0; i < 15; i++) {
        byte[] company;
        if (i % 2 == 0) {
          company = ford;
        } else {
          company = chrysler;
        }

        recordSale(hTable, company, i);
      }

      recordSale(hTable, toyota, 23);

      System.out.println(DebugUtil.getContent(hTable));

      Configuration configuration = testingUtility.getConfiguration();
      configuration.set("hut.mr.buffer.size", String.valueOf(10));
      Job job = new Job(configuration);
      UpdatesProcessingMrJob.initJob(tableName, new Scan(), StockSaleUpdateProcessor.class, job);

      job.waitForCompletion(true);

      System.out.println(DebugUtil.getContent(hTable));

      verifyLastSalesWithNativeScanner(hTable, ford, new int[] {14, 12, 10, 8, 6});
      verifyLastSalesWithNativeScanner(hTable, chrysler, new int[] {13, 11, 9, 7, 5});
      verifyLastSalesWithNativeScanner(hTable, toyota, new int[] {23});

    } finally { // TODO: do we really need try/finally block here?
      testingUtility.shutdownMiniMapReduceCluster();
    }
  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), SALE_CF);
    StockSaleUpdateProcessor processor = new StockSaleUpdateProcessor();

    // Writing data
    byte[] chrysler = Bytes.toBytes("chrysler");
    byte[] ford = Bytes.toBytes("ford");
    Delete deleteChrysler = new Delete(chrysler);

    // Verifying that delete operation succeeds when no data exists
    hTable.delete(deleteChrysler);

    verifyLastSales(hTable, processor, chrysler, new int[] {});
    verifyLastSales(hTable, processor, ford, new int[] {});
    recordSale(hTable, chrysler, 90);
    recordSale(hTable, chrysler, 100);
    recordSale(hTable, ford, 18);
    recordSale(hTable, chrysler, 120);

    HutUtil.delete(hTable, deleteChrysler);
    verifyLastSales(hTable, processor, chrysler, new int[] {});
    verifyLastSales(hTable, processor, ford, new int[] {18});

    recordSale(hTable, chrysler, 115);
    recordSale(hTable, ford, 22);
    recordSale(hTable, chrysler, 110);
    verifyLastSales(hTable, processor, chrysler, new int[] {110, 115});
    verifyLastSales(hTable, processor, ford, new int[] {22, 18});

    hTable.close();
  }

  private static void performUpdatesProcessingButWithoutDeletionOfProcessedRecords(final HTable hTable, UpdateProcessor updateProcessor) throws IOException {
    ResultScanner resultScanner =
            new HutResultScanner(hTable.getScanner(new Scan()), updateProcessor, hTable, true) {
              @Override
              void deleteProcessedRecords(byte[] firstInclusive, byte[] lastInclusive, byte[] processingResultToLeave) throws IOException {
              }
            };
    while (resultScanner.next() != null) {
      // DO NOTHING
    }
  }

  private static void recordSale(HTable hTable, byte[] company, int price) throws InterruptedException, IOException {
    Put put = new HutPut(company);
    put.add(SALE_CF, Bytes.toBytes("lastPrice0"), Bytes.toBytes(price));
    Thread.sleep(1); // sanity interval
    hTable.put(put);

  }

  private static void verifyLastSalesWithNativeScanner(HTable hTable, byte[] company, int[] prices) throws IOException {
    ResultScanner resultScanner = hTable.getScanner(getCompanyScan(company));
    Result result = resultScanner.next();
    verifyLastSales(result, prices);
  }

  private static void verifyLastSales(HTable hTable, UpdateProcessor updateProcessor, byte[] company, int[] prices) throws IOException {
    ResultScanner resultScanner =
            new HutResultScanner(hTable.getScanner(getCompanyScan(company)), updateProcessor);
    Result result = resultScanner.next();
    verifyLastSales(result, prices);
  }

  private static Scan getCompanyScan(byte[] company) {
    byte[] stopRow = Arrays.copyOf(company, company.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    // setting stopRow to fetch exactly the company needed, otherwise if company's data is absent scan will go further to the next one
    return new Scan(company, stopRow);
  }

  // pricesList - prices for all companies ordered alphabetically
  private static void verifyLastSalesForAllCompanies(HTable hTable, UpdateProcessor updateProcessor, int[][] pricesList) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(4);
    ResultScanner resultScanner = new HutResultScanner(hTable.getScanner(scan), updateProcessor);
    Result[] results = resultScanner.next(pricesList.length);
    for (int i = 0; i < pricesList.length; i++) {
      verifyLastSales(results.length > i ? results[i] : null, pricesList[i]);
    }
  }
  private static void verifyLastSalesWithCompation(HTable hTable, UpdateProcessor updateProcessor, byte[] company, int[] prices) throws IOException {
    ResultScanner resultScanner =
            new HutResultScanner(hTable.getScanner(getCompanyScan(company)), updateProcessor, hTable, true);
    Result result = resultScanner.next();
    verifyLastSales(result, prices);
  }

  private static void verifyLastSales(Result result, int[] prices) {
    // if there's no records yet, then prices are empty
    if (result == null) {
      Assert.assertTrue(prices.length == 0);
      return;
    }

    for (int i = 0; i < 5; i++) {
      byte[] lastStoredPrice = result.getValue(SALE_CF, Bytes.toBytes("lastPrice" + i));
      if (i < prices.length) {
        Assert.assertEquals(prices[i], Bytes.toInt(lastStoredPrice));
      } else {
        Assert.assertNull(lastStoredPrice);
      }
    }
  }

  @After
  public void after() throws IOException {
    testingUtility.shutdownMiniCluster();
    testingUtility = null;
  }
}
