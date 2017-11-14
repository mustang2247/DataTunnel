package com.bytegriffin.datatunnel.core;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.conf.SystemPropertiesParser;
import com.bytegriffin.datatunnel.conf.TaskDefine;
import com.bytegriffin.datatunnel.conf.TasksDefineXmlParser;
import com.bytegriffin.datatunnel.meta.*;
import com.bytegriffin.datatunnel.read.*;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.bytegriffin.datatunnel.write.*;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class TaskManager {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    private List<TaskDefine> tasks;

    public static TaskManager create() {
        return new TaskManager();
    }

    /**
     * 加载任务列表
     *
     * @return
     */
    public TaskManager loads() {
        SystemPropertiesParser properties = new SystemPropertiesParser();
        properties.load();
        TasksDefineXmlParser xml = new TasksDefineXmlParser();
        tasks = xml.load();
        tasks.forEach(task -> {
            load(task.getReaderDefines());
            load(task.getWriterDefines());
        });
        return this;
    }

    /**
     * 根据配置加载相应的Reader/Writer
     *
     * @param operators
     */
    private void load(List<? extends OperatorDefine> operators) {
        if (!SqlParser.isLoadedSystemSqlVariables()) {
            SqlParser.create().init(null);
        }
        operators.stream().filter(opt -> !Strings.isNullOrEmpty(opt.getType())).forEach(opt -> {
            try {
                DataType dataType = DataType.valueOf(opt.getType());
                switch (dataType) {
                    case mysql:
                        MysqlContext context = new MysqlContext();
                        context.init(opt);
                        if (opt.isReader()) {
                            opt.setReader(new MysqlReader());
                        } else if (opt.isWriter()) {
                            opt.setWriter(new MysqlWriter());
                        }
                        break;
                    case hbase:
                        HBaseContext hbcontext = new HBaseContext();
                        hbcontext.init(opt);
                        if (opt.isReader()) {
                            opt.setReader(new HBaseReader());
                        } else if (opt.isWriter()) {
                            opt.setWriter(new HBaseWriter());
                        }
                        break;
                    case mongodb:
                        MongoDBContext mongo = new MongoDBContext();
                        mongo.init(opt);
                        if (opt.isReader()) {
                            opt.setReader(new MongoDBReader());
                        } else if (opt.isWriter()) {
                            opt.setWriter(new MongoDBWriter());
                        }
                        break;
                    case kafka:
                        KafkaContext kafka = new KafkaContext();
                        kafka.init(opt);
                        if (opt.isReader()) {
                            opt.setReader(new KafkaConsumeReader());
                        } else if (opt.isWriter()) {
                            opt.setWriter(new KafkaProduceWriter());
                        }
                        break;
                    case redis:
                        RedisContext redis = new RedisContext();
                        redis.init(opt);
                        if (opt.isReader()) {
                            opt.setReader(new RedisReader());
                        } else if (opt.isWriter()) {
                            opt.setWriter(new RedisWriter());
                        }
                        break;
                    default:
                        break;
                }
            } catch (IllegalArgumentException e) {
                logger.error("xml配置文件中的数据类型[{}]出错。", opt.getType(), e);
                System.exit(1);
            }
        });
    }

    /**
     * 构建同步操作
     */
    public void buildSync() {
        Pipeline pipeline = new Pipeline();
        tasks.forEach(task -> {
            task.getWriterDefines().forEach(writerDefine -> {
                pipeline.addFirst(writerDefine.getWriter());
                Globals.operators.put(writerDefine.getWriter().hashCode(), writerDefine);
            });
            //先执行后面的操作
            task.getReaderDefines().forEach(readerDefine -> {
                pipeline.addFirst(readerDefine.getReader());
                Globals.operators.put(readerDefine.getReader().hashCode(), readerDefine);
            });
            Param param = new Param();
            param.setTaskDefine(task);
            pipeline.request(param);
        });
    }

}
