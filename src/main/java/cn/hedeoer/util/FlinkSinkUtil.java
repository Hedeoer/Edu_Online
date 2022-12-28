package cn.hedeoer.util;


import cn.hedeoer.annotation.annotation.NoSink;
import cn.hedeoer.bean.TableProcess;
import cn.hedeoer.common.Constant;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * ClassName: FlinkSinkUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/4 14:24
 */
public class FlinkSinkUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {

        return PhoenixSink.getSink();
    }

    public static SinkFunction<String> getKafkaSink(String topicName) {
        Properties pros = new Properties();
        pros.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
        pros.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        KafkaSerializationSchema<String> kafkaSeria = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(topicName, element.getBytes(StandardCharsets.UTF_8));
            }
        };

        return new FlinkKafkaProducer<String>(topicName, kafkaSeria, pros, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");

        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element,
                                                                    @Nullable Long timestamp) {
                        String topic = element.f1.getSinkTable();
                        return new ProducerRecord<>(topic, element.f0.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    public static <T> SinkFunction<T> getClickhouseSink(String tableName, Class<T> bean) {
        String url = Constant.CLICKHOUSE_URL;
        String driver = Constant.CLICKHOUSE_DRIVER;
        String userName = Constant.CLICKHOUSE_USERNAME;
        String password = Constant.CLICKHOUSE_PASSWORD;
        // 利用jdbcSink写出数据到clickhouse
        String querySql = "";
        StringBuilder builder = new StringBuilder();
        // 如何获取列名？
        Field[] fields = bean.getDeclaredFields();
        // 转列名字符串 TODO 属性的小驼峰转化为下划线
        String columns = StreamSupport.stream(Arrays.<Field>stream(fields).spliterator(), true)
                // 通过自定义注解形式 过滤出需要sink的字段
                .filter(s -> s.getAnnotation(NoSink.class) == null)
                .map( m -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,m.getName())).collect(Collectors.joining(","));

        builder.append("insert into ")
                .append(tableName + "(")
                .append(columns)
                .append(") values(")
                .append(columns.replaceAll("[^,]+", "?"))
                .append(")");

        querySql= builder.toString();
        System.out.println("ck插入数据sql: " + querySql);
        return JdbcSink.sink(querySql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T t) throws SQLException {

                        // 为占位符赋值
                        try {
                            Field[] fields = bean.getDeclaredFields();
                            // 变量k保证每个需要sink的field准确对应赋值
                            for (int i = 0, k = 1; i < fields.length; i++) {
                                Field field = fields[i];

                                if (field.getAnnotation(NoSink.class) == null) {
                                    field.setAccessible(true);
                                    Object columnValue = field.get(t);
                                    ps.setObject(k++,columnValue);
                                }
                            }
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(5000)
                        .withBatchSize(1024 * 1024)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withPassword(password)
                        .withUsername(userName)
                        .withUrl(url)
                        .withDriverName(driver)
                        .build()
        );
    }
}
