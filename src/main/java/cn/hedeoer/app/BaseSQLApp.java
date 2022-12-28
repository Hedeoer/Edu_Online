package cn.hedeoer.app;


import cn.hedeoer.common.Constant;
import cn.hedeoer.util.SqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * ClassName: BaseApp
 * Package: cn.hedeoer.exec.app.dim
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/2 18:12
 */
public abstract class BaseSQLApp {

    protected  abstract  void handle(StreamExecutionEnvironment env, StreamTableEnvironment TEnv);

    public void init(Integer port,
                     Integer parallelism,
                     String ckPathAndGroupIdAndJobName,
                     String topicName){

        System.setProperty("HADOOP_USER_NAME","atguigu");

        Configuration conf= new Configuration();
        conf.setInteger("rest.port",port);
//        conf.setString("pipeline.name",ckPathAndGroupIdAndJobName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/edu/" + ckPathAndGroupIdAndJobName);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        StreamTableEnvironment TEnv = StreamTableEnvironment.create(env);
        TEnv.getConfig().getConfiguration().setString("pipeline.name",ckPathAndGroupIdAndJobName);


        handle(env,TEnv);

    }


    /**
     * 从ods_db读取数据并封装成表
     * @param tEnv
     * @param groupId
     */
    public void readDataFromOdsDB(StreamTableEnvironment tEnv, String groupId){

        tEnv.executeSql("create table ods_db(" +
                " `database` string, " +
                " `table` string, " +
                " `type` string, " +
                " `data` map<string, string>, " +
                " `old` map<string, string>," +
                " `ts`       bigint," +
                "  `pt` as proctime()," +
                " `et` as TO_TIMESTAMP_LTZ(ts,0)," +
                "  watermark for `et` as `et` - interval '3' second " +
                " ) " +
                SqlUtil.getKafkaSource(Constant.KAFKA_ODS_DB_TOPIC,groupId,"json"));

    }


}
