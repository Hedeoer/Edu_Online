package cn.hedeoer.app;


import cn.hedeoer.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * ClassName: BaseApp
 * Package: cn.hedeoer.exec.app.dim
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/2 18:12
 */
public abstract class BaseStreamApp {

    protected  abstract  void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void init(Integer port,
                     Integer parallelism,
                     String ckPathAndGroupIdAndJobName,
                     String topicName){

        System.setProperty("HADOOP_USER_NAME","atguigu");

        Configuration conf= new Configuration();
        conf.setInteger("rest.port",port);
        conf.setString("pipeline.name",ckPathAndGroupIdAndJobName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/edu/" + ckPathAndGroupIdAndJobName);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        DataStreamSource<String> stream = env.fromSource(FlinkSourceUtil.getKafkaSource(ckPathAndGroupIdAndJobName, topicName),
                WatermarkStrategy.noWatermarks(),
                "kafka-source");




        handle(env,stream);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
