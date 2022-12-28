package cn.hedeoer.util;


import cn.hedeoer.common.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.io.IOException;

import static org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest;

/**
 * ClassName: FlinkSourceUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/2 16:50
 */
public class FlinkSourceUtil {


    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(new TypeHint<String>() {});
                    }

                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        return message != null ? new String(message) : null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }
                })
                .setProperty("isolation.level", "read_committed")
                .build();
        return build;
    }
}
