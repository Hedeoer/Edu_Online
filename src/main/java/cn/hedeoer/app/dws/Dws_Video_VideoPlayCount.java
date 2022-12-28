package cn.hedeoer.app.dws;

import cn.hedeoer.app.BaseStreamApp;
import cn.hedeoer.bean.Exam_QuestionAnswer;
import cn.hedeoer.bean.Video_PlayBean;
import cn.hedeoer.common.Constant;
import cn.hedeoer.function.EduSyncFunction;
import cn.hedeoer.util.FlinkSinkUtil;
import cn.hedeoer.util.HedeoerUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: Dwd_04_VideoPlayCount
 * Package: cn.hedeoer.app.dwd
 * Description:
 * 各个章节 :
 *  视频播放次数
 * 累计播放时长
 * 观看人数
 * 人均观看时长
 * @Author hedeoer
 * @Create 2022/12/26 20:53
 */
public class Dws_Video_VideoPlayCount extends BaseStreamApp {
    public static void main(String[] args) {
        new Dws_Video_VideoPlayCount()
                .init(
                        9077,
                        2,
                        "Dws_Video_VideoPlayCount",
                        Constant.TOPIC_DWD_TRAFFIC_VIDEO
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 水印
        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> startStream = stream.map(JSON::parseObject)
                // 分发水印
                .assignTimestampsAndWatermarks(WatermarkStrategy.<com.alibaba.fastjson.JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getLong("ts")  )
                        .withIdleness(Duration.ofSeconds(3)));
        // 获得章节id
        SingleOutputStreamOperator<JSONObject> chapterIdStream = getChapterId(startStream);
//        chapterIdStream.print();

        // 播放次数，播放时长
        SingleOutputStreamOperator<Video_PlayBean> streamOperator = processPlaytimesAndDuration(chapterIdStream);
//        streamOperator.print().setParallelism(10);
        // 分组 聚合
        SingleOutputStreamOperator<Video_PlayBean> aggStream = groupAndAgg(streamOperator);
//        aggStream.print();

        // 补充完整维度
        SingleOutputStreamOperator<Video_PlayBean> dimStream = completeDim(aggStream);

        //写出到ck
        dimStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_video_display_window", Video_PlayBean.class));
    }

    private SingleOutputStreamOperator<Video_PlayBean> completeDim(SingleOutputStreamOperator<Video_PlayBean> aggStream) {

        return AsyncDataStream.unorderedWait(
                aggStream,
                new EduSyncFunction<Video_PlayBean>() {
                    @Override
                    public String getTableName() {
                        return "DIM_CHAPTER_INFO";
                    }

                    @Override
                    public String getDimensionId(Video_PlayBean input) {
                        return input.getChapterId();
                    }

                    @Override
                    public void addDim(Video_PlayBean input, JSONObject dimensions) {
                        input.setChapterName(dimensions.get("CHAPTER_NAME")+ "");
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<Video_PlayBean> groupAndAgg(SingleOutputStreamOperator<Video_PlayBean> s) {
        return s.keyBy(Video_PlayBean :: getChapterId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .reduce(new ReduceFunction<Video_PlayBean>() {
                    @Override
                    public Video_PlayBean reduce(Video_PlayBean value1, Video_PlayBean value2) throws Exception {
                        value1.setNumberPlayback(value1.getNumberPlayback() + value1.getNumberPlayback());
                        value1.setDurationPlayback(value1.getDurationPlayback() + value2.getDurationPlayback());
                        value1.setViews(value1.getViews() + value1.getViews());
                        return value1;
                    }
                }, new ProcessWindowFunction<Video_PlayBean, Video_PlayBean, String, TimeWindow>() {

                    @Override
                    public void process(String s,
                                        ProcessWindowFunction<Video_PlayBean, Video_PlayBean,
                                                String,
                                                TimeWindow>.Context context,
                                        Iterable<Video_PlayBean> elements,
                                        Collector<Video_PlayBean> out) throws Exception {

                        // 封装窗口时间
                        String start = HedeoerUtil.toDateTime(context.window().getStart());
                        String end = HedeoerUtil.toDateTime(context.window().getEnd());

                        // 平均播放时长
                        Video_PlayBean next = elements.iterator().next();
                        Double avgDuration = next.getDurationPlayback() / (next.getViews() * 1.0D);
                        next.setTs(System.currentTimeMillis());
                        next.setDurationCapita(avgDuration);
                        next.setStt(start);
                        next.setEdt(end);

                        out.collect(next);


                    }
                });
    }

    private SingleOutputStreamOperator<Video_PlayBean> processPlaytimesAndDuration(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.keyBy(e -> e.getString("mid"))
                // TODO 实体bean错删除 doge！！
                .process(new KeyedProcessFunction<String, JSONObject, Video_PlayBean>() {

                    private ValueState<String> videoAndPlaySec;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        videoAndPlaySec = getRuntimeContext().getState(new ValueStateDescriptor<String>("videoAndPlaySec", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, Video_PlayBean>.Context ctx, Collector<Video_PlayBean> out) throws Exception {
                        // 视频播放单次时长超过20miao或者打开视频并在20s内关闭也算一次播放
                        long playSec = Long.parseLong(value.getString("play_sec"));
                        String videoId = value.getString("video_id");
                        // 播放次数
                        Long numberPlayback = 0L;
                        // 本次视频播放超过20sec，就为一次播放
                        if (playSec > 20L) {
                            numberPlayback = 1L;
                            // 更新状态
                            videoAndPlaySec.update(videoId + ":" + playSec);
                        }else {
                            String state = videoAndPlaySec.value();
                            // 上次视频播放 <= 20sec 并且 不为第一次观看该视频
                            if (playSec <= 20L && state != null) {
                                String[] videoAndSec = state.split(":");
                                String lastVideo = videoAndSec[0];
                                long lastSec = Long.parseLong(videoAndSec[1]);
                                String curVideo = value.getString("video_id");
                                // 更新状态
                                videoAndPlaySec.update(videoId + ":" + playSec);

                                // 更换了视频并且上次视频播放 <= 20sec，上次视频播放为一次播放
                                if (!curVideo.equals(lastVideo) && lastSec <= 20) {
                                    numberPlayback = 1L;
                                }
                            }
                        }

                        // 每个视频的观看人数
                        Long views = 1L;


                        Video_PlayBean bean = Video_PlayBean.builder()
                                .chapterId(value.getString("chapter_id"))
                                .videoId(value.getString("video_id"))
                                // 播放次数
                                .numberPlayback(numberPlayback)
                                // 累计播放时长
                                .durationPlayback(playSec)
                                // 观看人数
                                .views(views)
                                .build();
                        out.collect(bean);

                    }
                }).setParallelism(10);
    }

    private SingleOutputStreamOperator<JSONObject> getChapterId(SingleOutputStreamOperator<JSONObject> startStream) {
        return AsyncDataStream.unorderedWait(
                startStream,
                new EduSyncFunction<JSONObject>() {
                    @Override
                    public String getTableName() {
                        return "DIM_VIDEO_INFO";
                    }

                    @Override
                    public String getDimensionId(JSONObject input) {
                        return input.getString("video_id");
                    }

                    @Override
                    public void addDim(JSONObject input, JSONObject dimensions) {
                        input.put("chapter_id",dimensions.get("CHAPTER_ID"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }
}
