package cn.hedeoer.util;


import cn.hedeoer.common.Constant;

/**
 * ClassName: SqlUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/6 16:02
 */
public class SqlUtil {

    /**
     * 根据ods_db主题中的数据创建临时表ods_db的执行sql拼接 with部分
     * @param topic
     * @param groupId
     * @param format
     * @return
     */
    public static String getKafkaSource(String topic, String groupId, String ... format) {
        String formate = "json";
        if (format.length > 0) {
            formate = format[0];
        }
        return " with (" +
                " 'connector' = 'kafka'," +
                "'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"'," +
                " 'properties.group.id' = '"+groupId+"'," +
                " 'topic' = '"+topic+"'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'format' = '"+formate+"'," +
                " 'json.ignore-parse-errors' = 'true'" +
                " )";
    }

    public static String getUpsertKafka(String topic, String ... format) {
        String formate = "json";
        if (format.length > 0) {
            formate = format[0];
        }
        return " with (" +
                " 'connector' = 'upsert-kafka'," +
                "'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"'," +
                " 'topic' = '"+topic+"'," +
                " 'key.format' = '" + formate + "', " +
                " 'value.format' = '" + formate + "' " +
                " )";
    }

    public static String getKafkaSink(String topicName, String ... format) {
        String formate = "json";
        if (format.length > 0) {
            formate = format[0];
        }
        return " with (" +
                " 'connector' = 'kafka'," +
                "'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"'," +
                " 'topic' = '"+topicName+"'," +
                " 'format' = '"+formate+"'," +
                " 'json.ignore-parse-errors' = 'true'  " +
                " )";
    }
}
