package com.bytegriffin.datatunnel.write;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.sql.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class MysqlWriter implements Writeable {

    private static final Logger logger = LogManager.getLogger(MysqlWriter.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        DataSource dataSource = Globals.getDataSource(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
        write(dataSource, sqls);
        logger.info("线程[{}]调用MysqlWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    /**
     * 向数据库中批量写多条数据
     *
     * @param dataSource
     * @param sqls
     */
    private void write(DataSource dataSource, List<String> sqls) {
        try (Connection con = dataSource.getConnection();
             Statement stmt = con.createStatement()) {
            con.setAutoCommit(false);
            sqls.forEach(sql -> {
                try {
                    stmt.addBatch(sql);
                } catch (SQLException e) {
                    logger.error("不能执行更新sql: [{}]", sqls, e);
                }
            });
            stmt.executeBatch();
            con.commit();
        } catch (SQLException e) {
            logger.error("不能执行更新sql: [{}]", sqls, e);
        }
    }


}
