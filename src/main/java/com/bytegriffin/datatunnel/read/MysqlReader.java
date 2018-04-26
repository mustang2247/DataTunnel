package com.bytegriffin.datatunnel.read;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

public class MysqlReader implements Readable {

    private static final Logger logger = LogManager.getLogger(MysqlReader.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        DataSource dataSource = Globals.getDataSource(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        String newsql = SqlParser.getReadSql(opt.getValue());
        List<Record> results = select(dataSource, newsql);
        msg.setRecords(results);
        ctx.write(msg);
        logger.info("线程[{}]调用MysqlReader执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    private List<Record> select(DataSource dataSource, String sql) {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        List<Record> list = Lists.newArrayList();
        try (Connection con = dataSource.getConnection();
             PreparedStatement pstmt = con.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                Record result = new Record();
                List<Field> fields = Lists.newArrayList();
                for (int i = 1; i <= columnCount; i++) {
                    fields.add(new Field(md.getColumnLabel(i).toLowerCase(), rs.getObject(i)));
                }
                result.setFields(fields);
                list.add(result);
            }
        } catch (Exception e) {
            logger.error("MysqlReader查询数据时出错 : [{}]", sql, e);
        }
        return list;
    }

}
