package cn.hedeoer.app.dwd;

import cn.hedeoer.app.BaseStreamApp;
import cn.hedeoer.common.Constant;
import cn.hedeoer.util.FlinkSinkUtil;
import cn.hedeoer.util.HedeoerUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.immutable.Stream;

import java.util.HashMap;
import java.util.Map;


/**
 * ClassName: Dwd_03_LogTriffic
 * Package: cn.hedeoer.app.dwd
 * Description: 日志分流
 *
 * @Author hedeoer
 * @Create 2022/12/26 18:40
 */
public class Dwd_03_LogTriffic extends BaseStreamApp {

    private String display = "display";
    private String actions = "actions";
    private String page = "page";
    private String err = "err";
    private String video = "video";
    private String start = "start";

    public static void main(String[] args) {
        new Dwd_03_LogTriffic()
                .init(
                        7433,
                        2,
                        "Dwd_03_LogTriffic",
                        Constant.KAFKA_ODS_LOG_TOPIC
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 数据的etl （是否为json格式）
        SingleOutputStreamOperator<String> validateStream = validateJson(stream);

        // 2. 新老客户的纠正
        SingleOutputStreamOperator<JSONObject> startStream = validateNewUser(validateStream);
//        startStream.print();

        // 3. 6种日志数据的分流
        Map<String, DataStream<JSONObject>> map = splitLogStream(startStream);
//        map.get("video").print();
        // 4. 将日志数据写出到Kafka 的 topic
        wirteLogToKafka(map);

    }

    private void wirteLogToKafka(Map<String, DataStream<JSONObject>> map) {
        map.get(display).map(JSONAware :: toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        map.get(actions).map(JSONAware :: toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        map.get(err).map(JSONAware :: toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        map.get(page).map(JSONAware :: toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        map.get(video).map(JSONAware :: toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_VIDEO));
        map.get(start).map(JSONAware :: toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
    }

    private Map<String, DataStream<JSONObject>> splitLogStream(SingleOutputStreamOperator<JSONObject> stream) {
        // 使用五个测流和一个 主流输出六种日志数据
        // appvideo start error actions displays common

        OutputTag<JSONObject> videoTag = new OutputTag<JSONObject>("videoTag") {};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("pageTag") {};
        OutputTag<JSONObject> errorTag = new OutputTag<JSONObject>("errorTag") {};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("actionTag") {};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("displayTag") {};


        SingleOutputStreamOperator<JSONObject> logStream = stream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value,
                                       Context ctx,
                                       Collector<JSONObject> out) throws Exception {
                JSONObject common = value.getJSONObject("common");
                JSONObject start = value.getJSONObject("start");

                if (start != null) {
                    out.collect(start);

                } else {
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        // 新建外层jsonobj
                        JSONObject outer = new JSONObject();
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            // 放入每个display obj
                            outer.putAll(jsonObject);
                        }
                        // 放入ts 和 common 信息
                        outer.put("ts", value.getLong("ts"));
                        outer.putAll(common);
                        // 从value 中移除 displays
                        value.remove("displays");
                        // 测流写出
                        ctx.output(displayTag, outer);
                    }

                    // actions
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        JSONObject outer = new JSONObject();
                        for (int i = 0; i < actions.size(); i++) {
                            outer.putAll(actions.getJSONObject(i));
                        }
                        // common
                        outer.putAll(common);
                        // 从value 中移除 actions
                        value.remove("actions");
                        ctx.output(actionTag, outer);

                    }

                    // appvideo
                    JSONObject appVideo = value.getJSONObject("appVideo");
                    if (appVideo != null) {
                        JSONObject outer = new JSONObject();
                        outer.putAll(common);
                        outer.put("ts", value.getLong("ts"));
                        outer.putAll(appVideo);
                        // 从value 中移除 appVideo
                        value.remove("appVideo");
                        ctx.output(videoTag, outer);
                    }

                    // page
                    JSONObject page = value.getJSONObject("page");
                    if (page != null) {
                        JSONObject outer = new JSONObject();
                        outer.putAll(common);
                        outer.put("ts", value.getLong("ts"));
                        // 从value 中移除 page
                        value.remove("page");
                        ctx.output(pageTag, outer);
                    }

                    // error
                    JSONObject err = value.getJSONObject("err");
                    if (err != null) {
                        // 最后只剩下err，写出value全部即可
                        ctx.output(errorTag, value);

                    }
                }

            }
        });

        DataStream<JSONObject> display = logStream.getSideOutput(displayTag);
        DataStream<JSONObject> actions = logStream.getSideOutput(actionTag);
        DataStream<JSONObject> page = logStream.getSideOutput(pageTag);
        DataStream<JSONObject> err = logStream.getSideOutput(errorTag);
        DataStream<JSONObject> video = logStream.getSideOutput(videoTag);

        Map<String,DataStream<JSONObject>> map = new HashMap<String,DataStream<JSONObject>>();

        map.put(this.display,display);

        map.put(this.actions,actions);

        map.put(this.page,page);

        map.put(this.err,err);

        map.put(this.video,video);

        map.put(start,logStream);

        return map;



    }

    /**
     * 将本来就是老客户的数据标记is_new 更正为 0
     * @param validateStream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> validateNewUser(SingleOutputStreamOperator<String> validateStream) {
        return validateStream.map(JSON::parseObject)
                .keyBy(e -> e.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    // 存放每个用户的首次登录日期
                    private ValueState<String> firstLoginDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstLoginDate = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstLoginDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        String firstDate = firstLoginDate.value();
                        Long ts = Long.valueOf(value.getString("ts"));
                        String today = HedeoerUtil.toDate(ts);

                        // 新用户才有可能识别为老用户
                        if ("1".equals(isNew)) {
                            // 识别为新客户，但首次的登录日期和状态中存的首次登录日期不对，需要纠正
                            if (!today.equals(firstDate)) {
                                firstLoginDate.update(today);
                                value.getJSONObject("common").put("is_new","0");
                            }else{
                                 // 虽然首次的登录日期和状态中存的首次登录日期不对，但是这是用户的首次登录，此前状态中为null
                                if (firstLoginDate == null) {
                                    // 更新首次登录日期为今日
                                    firstLoginDate.update(today);
                                }
                            }

                        }else {
                            // 老用户需要选择今天以前一天作为首次登录日期
                            if ("0".equals(isNew)) {
                                String yesterday = HedeoerUtil.toDate(ts - 24 * 60 * 60 * 1000);
                                firstLoginDate.update(yesterday);
                            }
                        }
                        out.collect(value);

                    }
                });
    }

    private SingleOutputStreamOperator<String> validateJson(DataStreamSource<String> stream) {
        return stream.filter(e -> {
            try {
                JSON.parseObject(e);
                return true;
            } catch (Exception ex) {
                System.out.println("数据格式不为json！！");
                return false;
            }
        });
    }
}
