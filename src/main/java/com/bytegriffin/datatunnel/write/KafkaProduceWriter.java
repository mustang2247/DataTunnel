package com.bytegriffin.datatunnel.write;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.KafkaContext;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;

/**
 * 生产者负责讲数据写到Kafka中
 */
public class KafkaProduceWriter implements Writeable {

    private static final Logger logger = LogManager.getLogger(KafkaProduceWriter.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        Properties properties = Globals.getKafkaProperties(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
        String topic = properties.getProperty(KafkaContext.topic);
        write(properties, topic, sqls);
        logger.info("线程[{}]调用KafkaProduceWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    /**
     * 目前只支持insert操作
     *
     * @param properties
     */
    private void write(Properties properties, String topicName, List<String> sqls) {
        Producer<String, String> producer = new KafkaProducer<>(properties);
        sqls.forEach(sql -> {
            List<Field> ff = SqlMapper.insert(sql).getFields();
            producer.send(new ProducerRecord<>(topicName, ff.get(0).getFieldValue().toString(), ff.get(1).getFieldValue().toString()));
        });
        producer.close();
    }

}
