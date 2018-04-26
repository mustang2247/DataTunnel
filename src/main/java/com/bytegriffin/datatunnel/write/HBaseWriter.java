package com.bytegriffin.datatunnel.write;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.HBaseContext;
import com.bytegriffin.datatunnel.sql.*;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HBaseWriter implements Writeable {

    private static final Logger logger = LogManager.getLogger(HBaseWriter.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        Connection connection = Globals.getHBaseConnection(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
        write(connection, sqls);
        logger.info("线程[{}]调用MysqlWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    /**
     * 向HBase数据库中批量写多条数据<br>
     * 编写HBase更新Sql时需要注意:<br>
     * 1.sql中必须包含row，否则put/delete操作不能构造函数<br>
     * 2.sql中必须包含column family，且column family要与column之间用句号隔开 <br>
     * insert sql example : insert into table1 (row, cf.col1) values ('${row}','${cf.col1}')，
     * 注意要加row和column family  <br>
     * update sql example : UPDATE table2 SET cf.name = 'Fred' WHERE row = '${row}' ，
     * where条件只能跟row，不用加其他and/or条件，因为它本来就支持增量更新的<br>
     * delete sql example : delete from table3 where row='${row}'<br>
     * 所有${row}变量都需要对应select查询中的row或者其它主键 <br>
     *
     * @param connection
     * @param sqls
     */
    private void write(Connection connection, List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return;
        }
        try {
            String firstSql = sqls.get(0).toLowerCase().trim();
            if (firstSql.contains("delete")) {//delete操作：只支持row作为查询条件
                List<Delete> deletes = Lists.newArrayList();
                DeleteObject firstObj = SqlMapper.delete(firstSql);
                String tablename = firstObj.getTableName();
                sqls.forEach(sql -> {
                    DeleteObject deleteobj = SqlMapper.delete(sql);
                    if (Strings.isNullOrEmpty(deleteobj.getWhere())) {
                        logger.error("where条件中必须包含row字段，请重新设置。");
                        return;
                    }
                    String right = Splitter.on("=").trimResults().omitEmptyStrings().splitToList(deleteobj.getWhere()).get(1);
                    Delete delete = new Delete(Bytes.toBytes(SqlParser.removeSqlQuotes(right)));
                    deletes.add(delete);
                });
                Table table = connection.getTable(TableName.valueOf(tablename));
                table.delete(deletes);
            } else if (firstSql.contains("insert")) {//insert操作：注意row字段需要reader端设置
                List<Put> puts = Lists.newArrayList();
                InsertObject firstObj = SqlMapper.insert(firstSql);
                String tablename = firstObj.getTableName();
                sqls.forEach(sql -> {
                    InsertObject insertObj = SqlMapper.insert(sql);
                    List<Field> fields = insertObj.getFields();
                    addBatch(fields, fields.get(0).getFieldValue().toString(), puts);
                });
                Table table = connection.getTable(TableName.valueOf(tablename));
                table.put(puts);
            } else if (firstSql.contains("update")) {//update操作：无需where条件，hbase会增量更新，注意row字段需要reader端设置
                List<Put> puts = Lists.newArrayList();
                UpdateObject firstObj = SqlMapper.update(firstSql);
                String tablename = firstObj.getTableName();
                sqls.forEach(sql -> {
                    UpdateObject updateObj = SqlMapper.update(sql);
                    List<Field> fields = updateObj.getFields();
                    String rowvalue = Splitter.on("=").trimResults().omitEmptyStrings().splitToList(updateObj.getWhere()).get(1);
                    addBatch(fields, rowvalue, puts);
                });
                Table table = connection.getTable(TableName.valueOf(tablename));
                table.put(puts);
            }
        } catch (Exception e) {
            logger.error("不能执行HBase更新sql: {}", sqls, e);
        }
    }

    /**
     * 增加put批量操作
     *
     * @param fields
     * @param rowValue
     * @param puts
     */
    private void addBatch(List<Field> fields, String rowValue, List<Put> puts) {
        byte[] row = Bytes.toBytes(SqlParser.removeSqlQuotes(rowValue));
        fields.stream().filter(field -> !field.getFieldName().contains(HBaseContext.row_key)).forEach(field -> {
            Put put = new Put(row);
            List<String> allcolumn = Splitter.on(HBaseContext.column_familiy_split).trimResults().omitEmptyStrings().splitToList(field.getFieldName().toString());
            put.addColumn(Bytes.toBytes(allcolumn.get(0)), Bytes.toBytes(allcolumn.get(1)), Bytes.toBytes(SqlParser.removeSqlQuotes(field.getFieldValue().toString())));
            puts.add(put);
        });
    }

}
