package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by balachandrapai on 19.06.17.
 */
public class HBaseMCCTest {

    @Test
    public void initialize(){


        //Primary Cluster
        Configuration primaryConfig = HBaseConfiguration.create();

        primaryConfig.set("hbase.zookeeper.quorum", "cdhmaster1");
        primaryConfig.set("hbase.zookeeper.property.clientPort", "2181");
        primaryConfig.set("hadoop.security.authentication", "kerberos");
        primaryConfig.set("hbase.security.authentication", "kerberos");
        primaryConfig.set("hbase.master.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
        primaryConfig.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
        UserGroupInformation.setConfiguration(primaryConfig);
        System.out.println("Principal Authentication: ");
        final String user = "hbase/cdhmaster1@EXAMPLE.COM";
        final String keyPath = "/run/cloudera-scm-agent/process/71-hbase-REGIONSERVER/hbase.keytab";
        try {
            UserGroupInformation.loginUserFromKeytab(user, keyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //failover Cluster
        Configuration failover = HBaseConfiguration.create();

        failover.set("hbase.zookeeper.quorum", "cdhmaster2");
        failover.set("hbase.zookeeper.property.clientPort", "2181");
        failover.set("hadoop.security.authentication", "kerberos");
        failover.set("hbase.security.authentication", "kerberos");
        failover.set("hbase.master.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
        failover.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
        UserGroupInformation.setConfiguration(primaryConfig);
        System.out.println("Principal Authentication: ");
        final String user2 = "hbase/cdhmaster2@EXAMPLE.COM";
        final String keyPath2 = "/run/cloudera-scm-agent/process/47-hbase-MASTER/hbase.keytab";
        try {
            UserGroupInformation.loginUserFromKeytab(user, keyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }


//        //failover Cluster2
//        Configuration failover2 = HBaseConfiguration.create();
//
//        failover2.set("hbase.zookeeper.quorum", "cdhmaster3");
//        failover2.set("hbase.zookeeper.property.clientPort", "2181");
//        failover2.set("hadoop.security.authentication", "Kerberos");
//        failover2.set("hbase.security.authentication", "kerberos");
//        failover2.set("hbase.master.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
//        failover2.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
//        UserGroupInformation.setConfiguration(primaryConfig);
//        System.out.println("Principal Authentication: ");
//        final String user3 = "hbase/cdhmaster3@EXAMPLE.COM";
//        final String keyPath3 = "/home/jing/jing.keytab";
//        try {
//            UserGroupInformation.loginUserFromKeytab(user, keyPath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Map<String, Configuration> failoverClusters = new HashMap<String, Configuration>();
        failoverClusters.put("failover", failover);
//        failoverClusters.put("failover2", failover2);

        Configuration combinedConfig = HBaseMultiClusterConfigUtil.combineConfigurations(primaryConfig, failoverClusters);

        combinedConfig.setInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 0);

        try {
            Connection connection = HConnectionManagerMultiClusterWrapper.createConnection(combinedConfig);

            Table multiTable = connection.getTable(TableName.valueOf("t1"));

            Get get = new Get(Bytes.toBytes("row1"));

            Result result = multiTable.get(get);


        } catch (IOException e) {
            e.printStackTrace();
        }



    }


}
