package com.bytegriffin.datatunnel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHBase {

    private static String zkaddress = "localhost:2181";
    private static String tablename = "t1";
    private static String colomnfamily = "cf";

    /**
     * 获取链接
     *
     * @return
     */
    public static Connection getConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkaddress);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 创建表
     * create 'table_name','column_family'
     */
    public static void createTable() {
        Connection conn = getConnection();
        TableName tableName = TableName.valueOf(tablename);
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("表已经存在，先删除");
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor family = new HColumnDescriptor(colomnfamily);
            hTableDescriptor.addFamily(family);
            admin.createTable(hTableDescriptor);
            System.out.println("表创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     * put <table>,<rowkey>,<family:column>,<value>
     */
    public static void insertData() {
        Connection conn = getConnection();
        try {
            Table table = conn.getTable(TableName.valueOf(tablename));
            Put put = new Put(Bytes.toBytes("row"));
            put.addColumn(Bytes.toBytes(colomnfamily), Bytes.toBytes("column1"), Bytes.toBytes("123123123"));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询表
     * get <table>,<rowkey>,[<family:column>,....]
     */
    public static void query() {
        Connection conn = getConnection();
        try {
            Table table = conn.getTable(TableName.valueOf(tablename));
            ResultScanner scanner = table.getScanner(new Scan());
            scanner.forEach(result -> {
                System.out.println("rowkey=" + new String(result.getRow()));
                result.listCells().forEach(cell -> {
                    String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("colomnfamily=" + columnFamily + " qualifier=" + columnName + " value=" + columnValue);
                });
            });

//	        Get get = new Get("row".getBytes());
//	        Result result = table.get(get);
//	        System.out.println("row key is:" + new String(result.getRow()));

//	        result.listCells().forEach(cell -> {
//	        	System.out.println("colomnfamily="+new String(cell.getFamilyArray()) 
//	        			+ "qualifier=" + new String(cell.getQualifierArray())
//						+ "value=" + new String(cell.getValueArray()));
//	        });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        query();
    }

}
