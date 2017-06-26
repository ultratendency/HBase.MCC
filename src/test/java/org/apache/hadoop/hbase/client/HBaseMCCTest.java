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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by balachandrapai on 19.06.17.
 */
public class HBaseMCCTest {

    Configuration combinedConfig;
    Connection connection;


    @Before
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
        final String keyPath = "/home/cloudera/Desktop/Security/cdhmaster1/hbase.keytab";
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
        final String keyPath2 = "/home/cloudera/Desktop/Security/cdhmaster2/hbase.keytab";
        try {
            UserGroupInformation.loginUserFromKeytab(user2, keyPath2);
        } catch (IOException e) {
            e.printStackTrace();
        }


//        //failover Cluster2
//        Configuration failover2 = HBaseConfiguration.create();
//
//        failover2.set("hbase.zookeeper.quorum", "cdhmaster3");
//        failover2.set("hbase.zookeeper.property.clientPort", "2181");
//        failover2.set("hadoop.security.authentication", "kerberos");
//        failover2.set("hbase.security.authentication", "kerberos");
//        failover2.set("hbase.master.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
//        failover2.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@EXAMPLE.COM");
//        UserGroupInformation.setConfiguration(primaryConfig);
//        System.out.println("Principal Authentication: ");
//        final String user3 = "hbase/cdhmaster3@EXAMPLE.COM";
//        final String keyPath3 = "/hhome/cloudera/Desktop/Security/cdhmaster3/hbase.keytab";
//        try {
//            UserGroupInformation.loginUserFromKeytab(user3, keyPath3);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Map<String, Configuration> failoverClusters = new HashMap<String, Configuration>();

        failoverClusters.put("failover", failover);
//        failoverClusters.put("failover2", failover2);


        combinedConfig = HBaseMultiClusterConfigUtil.combineConfigurations(primaryConfig, failoverClusters);
        //combinedConfig.setInt(ConfigConst.HBASE_WAIT_TIME_BEFORE_TRYING_PRIMARY_AFTER_FAILURE, 0);

        try {
             connection = HConnectionManagerMultiClusterWrapper.createConnection(combinedConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void readFromMCC(){

        try {
            Table multiTable = connection.getTable(TableName.valueOf("t1"));
            Get get1 = new Get(Bytes.toBytes("row1"));

            Result result = multiTable.get(get1);

            assertFalse(result.isEmpty());



        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void writeToMCC(){

        try {
            Table multiTable = connection.getTable(TableName.valueOf("t1"));
            Put put1 = new Put(Bytes.toBytes("row4"));

            put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("Data"));
            multiTable.put(put1);

            Get get1 = new Get(Bytes.toBytes("row4"));
            Result result = multiTable.get(get1);

            assertFalse(result.isEmpty());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
