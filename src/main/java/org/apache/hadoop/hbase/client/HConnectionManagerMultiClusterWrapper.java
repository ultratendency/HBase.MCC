package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class HConnectionManagerMultiClusterWrapper {

  public static Connection createConnection(Configuration conf)
      throws IOException {

    Logger LOG = Logger.getLogger(HConnectionManagerMultiClusterWrapper.class);

    Collection < String > failoverClusters = conf
            .getStringCollection(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);

    if (failoverClusters.size() == 0) {
      LOG.info(" -- Getting a signle cluster connection !!");
      return ConnectionFactory.createConnection(conf);
    } else {

      Map<String, Configuration> configMap = HBaseMultiClusterConfigUtil
          .splitMultiConfigFile(conf);

      LOG.info(" -- Getting primary Connction");
      Connection primaryConnection = ConnectionFactory
          .createConnection(configMap
              .get(HBaseMultiClusterConfigUtil.PRIMARY_NAME));
      LOG.info(" --- Got primary Connction");

      ArrayList<Connection> failoverConnections = new ArrayList<Connection>();

      for (Entry<String, Configuration> entry : configMap.entrySet()) {
        if (!entry.getKey().equals(HBaseMultiClusterConfigUtil.PRIMARY_NAME)) {
          LOG.info(" -- Getting failure Connction");
          failoverConnections.add(ConnectionFactory.createConnection(entry
              .getValue()));
          LOG.info(" --- Got failover Connction");
        }
      }
      
      return new HConnectionMultiCluster(conf, primaryConnection,
          failoverConnections.toArray(new Connection[0]));
    }
  }
}
