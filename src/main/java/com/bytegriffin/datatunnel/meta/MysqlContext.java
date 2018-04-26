package com.bytegriffin.datatunnel.meta;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MysqlContext implements Initializer {

    private static final Logger logger = LogManager.getLogger(MysqlContext.class);

    @Override
    public void init(OperatorDefine operator) {
        String jdbc = operator.getAddress();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbc);
        config.setMaximumPoolSize(50);
        config.setConnectionTestQuery("SELECT 1");
        try {
            HikariDataSource datasource = new HikariDataSource(config);
            if (datasource.isClosed()) {
                logger.error("任务[{}]加载组件MysqlContext[{}]没有连接成功。", operator.getName(), operator.getId());
                System.exit(1);
            }
            Globals.setDataSource(operator.getKey(), datasource);
            logger.info("任务[{}]加载组件MysqlContext[{}]的初始化完成。", operator.getName(), operator.getId());
        } catch (RuntimeException re) {
            logger.error("任务[{}]加载组件MysqlContext[{}]没有连接成功。", operator.getName(), operator.getId(), re);
        }
    }

}
