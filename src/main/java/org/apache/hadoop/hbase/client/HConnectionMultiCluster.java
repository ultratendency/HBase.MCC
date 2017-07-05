package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService.BlockingInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class HConnectionMultiCluster implements Connection {

  Connection primaryConnection;
  Connection[] failoverConnections;
  Configuration originalConfiguration;
  boolean isMasterMaster;
  int waitTimeBeforeAcceptingResults;
  int waitTimeBeforeRequestingFailover;
  int waitTimeBeforeMutatingFailover;
  int waitTimeBeforeMutatingFailoverWithPrimaryException;
  int waitTimeBeforeAcceptingBatchResults;
  int waitTimeBeforeRequestingBatchFailover;
  int waitTimeBeforeMutatingBatchFailover;
  int waitTimeFromLastPrimaryFail;

  static final Log LOG = LogFactory.getLog(HConnectionMultiCluster.class);
  
  ExecutorService executor;

  public HConnectionMultiCluster(Configuration originalConfiguration,
      Connection primaryConnection, Connection[] failoverConnections) {
    this.primaryConnection = primaryConnection;
    this.failoverConnections = failoverConnections;
    this.originalConfiguration = originalConfiguration;
    this.isMasterMaster = originalConfiguration
        .getBoolean(
            ConfigConst.HBASE_FAILOVER_MODE_CONFIG,
            false);
    this.waitTimeBeforeAcceptingResults = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_ACCEPTING_FAILOVER_RESULT_CONFIG,
            100);
    this.waitTimeBeforeMutatingFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_FAILOVER_CONFIG,
            100);
    this.waitTimeBeforeMutatingFailoverWithPrimaryException = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_FAILOVER_WITH_PRIMARY_EXCEPTION_CONFIG,
            0);
    this.waitTimeBeforeRequestingFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_REQUEST_FAILOVER_CONFIG,
            100);
    this.waitTimeBeforeAcceptingBatchResults = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_ACCEPTING_FAILOVER_BATCH_RESULT_CONFIG,
            100);
    this.waitTimeBeforeRequestingBatchFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_MUTATING_BATCH_FAILOVER_CONFIG,
            100);
    this.waitTimeBeforeMutatingBatchFailover = originalConfiguration
        .getInt(
            ConfigConst.HBASE_WAIT_TIME_BEFORE_REQUEST_BATCH_FAILOVER_CONFIG,
            100);
    this.waitTimeFromLastPrimaryFail = originalConfiguration
            .getInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 5000);

    executor = Executors.newFixedThreadPool(originalConfiguration.getInt(ConfigConst.HBASE_MULTI_CLUSTER_CONNECTION_POOL_SIZE, 20));
  }

  public void abort(String why, Throwable e) {
    primaryConnection.abort(why, e);
    for (Connection failOverConnection : failoverConnections) {
      failOverConnection.abort(why, e);
    }
  }

  public boolean isAborted() {
    return primaryConnection.isAborted();
  }

  public void close() throws IOException {

    Exception lastException = null;
    try {
      primaryConnection.close();
    } catch (Exception e) {
      LOG.error("Exception while closing primary", e);
      lastException = e;
    }
    for (Connection failOverConnection : failoverConnections) {
      try {
        failOverConnection.close();
      } catch (Exception e) {
        LOG.error("Exception while closing primary", e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new IOException(lastException);
    }
  }

  public Configuration getConfiguration() {
    return originalConfiguration;
  }


  public Table getTable(String tableName) throws IOException {
    return this.getTable(Bytes.toBytes(tableName));
  }


  public Table getTable(byte[] tableName) throws IOException {
    return this.getTable(TableName.valueOf(tableName));
  }


  public Table getTable(TableName tableName) throws IOException {
    LOG.info(" -- getting primaryHTable" + primaryConnection.getConfiguration().get("hbase.zookeeper.quorum"));
    Table primaryHTable = primaryConnection.getTable(tableName);
    primaryConnection.getBufferedMutator(tableName).flush();

    LOG.info(" --- got primaryHTable");
    ArrayList<Table> failoverHTables = new ArrayList<Table>();
    for (Connection failOverConnection : failoverConnections) {
      LOG.info(" -- getting failoverHTable:" + failOverConnection.getConfiguration().get("hbase.zookeeper.quorum"));

      Table htable = failOverConnection.getTable(tableName);
      primaryConnection.getBufferedMutator(tableName).flush();

      failoverHTables.add(htable);
      LOG.info(" --- got failoverHTable");
    }

    return new HTableMultiCluster(originalConfiguration, primaryHTable,
        failoverHTables, isMasterMaster, 
        waitTimeBeforeAcceptingResults,
        waitTimeBeforeRequestingFailover,
        waitTimeBeforeMutatingFailover,
        waitTimeBeforeMutatingFailoverWithPrimaryException,
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeRequestingBatchFailover,
        waitTimeBeforeMutatingBatchFailover,
            waitTimeFromLastPrimaryFail);
  }

  public Table getTable(String tableName, ExecutorService pool)
      throws IOException {
    return this.getTable(TableName.valueOf(tableName), pool);
  }

  public Table getTable(byte[] tableName, ExecutorService pool)
      throws IOException {
    return this.getTable(TableName.valueOf(tableName), pool);
  }

  public Table getTable(TableName tableName, ExecutorService pool)
      throws IOException {
    Table primaryHTable = primaryConnection.getTable(tableName, pool);
    ArrayList<Table> failoverHTables = new ArrayList<Table>();
    for (Connection failOverConnection : failoverConnections) {
      failoverHTables.add(failOverConnection.getTable(tableName, pool));
    }

    return new HTableMultiCluster(originalConfiguration, primaryHTable,
        failoverHTables, isMasterMaster, 
        waitTimeBeforeAcceptingResults,
        waitTimeBeforeRequestingFailover,
        waitTimeBeforeMutatingFailover,
        waitTimeBeforeMutatingFailoverWithPrimaryException,
        waitTimeBeforeAcceptingBatchResults,
        waitTimeBeforeRequestingBatchFailover,
        waitTimeBeforeMutatingBatchFailover,
            waitTimeFromLastPrimaryFail);
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return primaryConnection.getBufferedMutator(tableName);
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
    return primaryConnection.getBufferedMutator(bufferedMutatorParams);
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return primaryConnection.getRegionLocator(tableName);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return primaryConnection.getAdmin();
  }

  public boolean isMasterRunning() throws MasterNotRunningException,
          ZooKeeperConnectionException, IOException {
    return primaryConnection.getAdmin().getClusterStatus().getMaster() != null;
  }

  public boolean isTableEnabled(TableName tableName) throws IOException {
    return primaryConnection.getAdmin().isTableEnabled(tableName);
  }

  @Deprecated
  public
  boolean isTableEnabled(byte[] tableName) throws IOException {
    return primaryConnection.getAdmin().isTableEnabled(TableName.valueOf(tableName));
  }

  public boolean isTableDisabled(TableName tableName) throws IOException {
    return primaryConnection.getAdmin().isTableDisabled(tableName);
  }

  @Deprecated
  public
  boolean isTableDisabled(byte[] tableName) throws IOException {
    return primaryConnection.getAdmin().isTableDisabled(TableName.valueOf(tableName));
  }

  public boolean isTableAvailable(TableName tableName) throws IOException {
    return primaryConnection.getAdmin().isTableAvailable(tableName);
  }

  @Deprecated
  public
  boolean isTableAvailable(byte[] tableName) throws IOException {
    return primaryConnection.getAdmin().isTableAvailable(TableName.valueOf(tableName));
  }

  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys)
      throws IOException {
    return primaryConnection.getAdmin().isTableAvailable(tableName, splitKeys);
  }

  @Deprecated
  public
  boolean isTableAvailable(byte[] tableName, byte[][] splitKeys)
      throws IOException {
    return primaryConnection.getAdmin().isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  public HTableDescriptor[] listTables() throws IOException {
    return primaryConnection.getAdmin().listTables();
  }

  @Deprecated
  public String[] getTableNames() throws IOException {
    TableName[] tableNames = primaryConnection.getAdmin().listTableNames();
    String[] tNames = new String[tableNames.length];
    int i =0;
    for (TableName tn: tableNames) {
      tNames[i] = tn.toString();
      i++;
    }
    return tNames;
  }

  public TableName[] listTableNames() throws IOException {
    return primaryConnection.getAdmin().listTableNames();
  }

  public HTableDescriptor getHTableDescriptor(TableName tableName)
      throws IOException {
    return primaryConnection.getAdmin().getTableDescriptor(tableName);
  }

  @Deprecated
  public HTableDescriptor getHTableDescriptor(byte[] tableName) throws IOException {
    return primaryConnection.getAdmin().getTableDescriptor(TableName.valueOf(tableName));
  }

  public HRegionLocation getRegionLocation(TableName tableName, byte[] row,
      boolean reload) throws IOException {
    return primaryConnection.getRegionLocator(tableName).getRegionLocation(row, reload);
  }

  @Deprecated
  public
  HRegionLocation getRegionLocation(byte[] tableName, byte[] row, boolean reload)
      throws IOException {
    return primaryConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(row, reload);
  }

  @Deprecated
  public void processBatch(List<? extends Row> actions, TableName tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    throw new RuntimeException("processBatch not supported in " + this.getClass());
    
  }

  @Deprecated
  public
  void processBatch(List<? extends Row> actions, byte[] tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    primaryConnection.getTable(TableName.valueOf(tableName)).batch(actions, results);
  }

  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list,
      TableName tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    primaryConnection.getTable(tableName).batchCallback(list, results, callback);
  }

  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list,
      byte[] tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    primaryConnection.getTable(TableName.valueOf(tableName)).batchCallback(list, results, callback);

  }

  public HTableDescriptor[] getHTableDescriptorsByTableName(
      List<TableName> tableNames) throws IOException {
    HTableDescriptor tdArr[] = new HTableDescriptor[tableNames.size()];
    int i=0;
    for (TableName tn: tableNames) {
      tdArr[i] = primaryConnection.getAdmin().getTableDescriptor(tn);
      i++;
    }
    return tdArr;
  }

  @Deprecated
  public
  HTableDescriptor[] getHTableDescriptors(List<String> tableNames)
      throws IOException {
    HTableDescriptor tdArr[] = new HTableDescriptor[tableNames.size()];
    int i=0;
    for (String tn: tableNames) {
      tdArr[i] = primaryConnection.getAdmin().getTableDescriptor(TableName.valueOf(tn));
      i++;
    }
    return tdArr;
  }

  public boolean isClosed() {
    return primaryConnection.isClosed();
  }

  @Deprecated
  public
  MasterKeepAliveConnection getKeepAliveMasterService()
      throws MasterNotRunningException {
    // TODO Auto-generated method stub
    return null;
  }
}
