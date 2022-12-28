package cn.hedeoer.common;

/**
 * ClassName: Constant
 * Package: cn.hedeoer.common
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/24 12:23
 */
public class Constant {
    // kafka的broker节点
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
    // ++++++++++++++++++++++++++++ods++++++++++++++++++++++++++++++++++++++++++++++
    // ods层的业务数据的topic
    public static final String KAFKA_ODS_DB_TOPIC = "ods_db";
    // ods层的日志数据的topic
    public static final String KAFKA_ODS_LOG_TOPIC = "ods_log";



    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop162:8123/edu";
    public static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";
    public static final String CLICKHOUSE_USERNAME = "default";
    public static final String CLICKHOUSE_PASSWORD = "";



    // phoenix的driver全类名
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // phoenix的url
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";



    public static final String MYSQLURL = "jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false";
    public static final String MYSQLDRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQLUSERNAME = "root";
    public static final String MYSQLPASSWORD = "aaaaaa";


    public static final String EDU_DWD_TOPIC_USER_ANSWER_PAPER = "edu_dwd_topic_user_answer_paper";
    public static final String EDU_DWD_TOPIC_USER_ANSWER_QUESTION = "edu_dwd_topic_user_answer_question";
    // dwd层曝光日志数据topic
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "edu_dwd_traffic_display";
    // dwd层用户行为日志数据topic
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "edu_dwd_traffic_action";
    // dwd层错误日志数据topic
    public static final String TOPIC_DWD_TRAFFIC_ERR = "edu_dwd_traffic_err";
    // dwd层页面日志数据topic
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "edu_dwd_traffic_page";
    // dwd层启动日志数据topic
    public static final String TOPIC_DWD_TRAFFIC_START = "edu_dwd_traffic_start";
    // dwd层视频日志数据topic
    public static final String TOPIC_DWD_TRAFFIC_VIDEO = "edu_dwd_traffic_video";
}
