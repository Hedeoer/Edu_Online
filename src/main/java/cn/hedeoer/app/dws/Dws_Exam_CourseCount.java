package cn.hedeoer.app.dws;

import cn.hedeoer.app.BaseStreamApp;
import cn.hedeoer.bean.Exam_CountCourse;
import cn.hedeoer.common.Constant;
import cn.hedeoer.function.EduSyncFunction;
import cn.hedeoer.util.FlinkSinkUtil;
import cn.hedeoer.util.HedeoerUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: Dws_Exam_CourseCount
 * Package: cn.hedeoer.app.dws
 * Description:
 * 课程id',
 * 考试人数',
 * 平均分',
 * 平均时长',
 *
 * @Author hedeoer
 * @Create 2022/12/25 19:14
 */
public class Dws_Exam_CourseCount extends BaseStreamApp {

    public static void main(String[] args) {
        new Dws_Exam_CourseCount()
                .init(
                        7893,
                        2,
                        "Dws_Exam_CourseCount",
                        Constant.EDU_DWD_TOPIC_USER_ANSWER_PAPER
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 预先转成pojo
        SingleOutputStreamOperator<JSONObject> preStream = preComingObject(stream);

        // 由数据中的paper_id先获得courseId
        SingleOutputStreamOperator<JSONObject> courseIdDimStream = addDimensionByPhoenix(preStream);

        // JSONObject --> pojo
        SingleOutputStreamOperator<Exam_CountCourse> beanStream = Json2Pojo(courseIdDimStream);
//        beanStream.print();

        // 分组聚合
        SingleOutputStreamOperator<Exam_CountCourse> aggStream = groupAndAgg(beanStream);
//        aggStream.print();

        // 补充完整维度
        SingleOutputStreamOperator<Exam_CountCourse> comDimStream = completeDim(aggStream);
//        comDimStream.print();


        // 写入clickhouse
        comDimStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_exam_course_window",Exam_CountCourse.class));


    }

    /**
     * 补充course_name的完整维度
     * @param aggStream
     * @return
     */
    private SingleOutputStreamOperator<Exam_CountCourse> completeDim(SingleOutputStreamOperator<Exam_CountCourse> aggStream) {
        return AsyncDataStream.unorderedWait(
                aggStream,
                new EduSyncFunction<Exam_CountCourse>() {
                    @Override
                    public String getTableName() {
                        return "DIM_COURSE_INFO";
                    }

                    @Override
                    public String getDimensionId(Exam_CountCourse input) {
                        return input.getCourseId();
                    }

                    @Override
                    public void addDim(Exam_CountCourse input, JSONObject dimensions) {
                        input.setCourseName(dimensions.getString("COURSE_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }

    /**
     * 计算平均时长和平均分数
     * @param beanStream
     * @return
     */
    private SingleOutputStreamOperator<Exam_CountCourse> groupAndAgg(SingleOutputStreamOperator<Exam_CountCourse> beanStream) {
        return beanStream.keyBy(Exam_CountCourse :: getCourseId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Exam_CountCourse, Exam_CountCourse, Exam_CountCourse>() {
                    @Override
                    public Exam_CountCourse createAccumulator() {
                        return new Exam_CountCourse("",
                                "",
                                "",
                                "",
                                "",
                                0D,
                                0D,
                                0L,
                                0D,
                                0D,
                                0L);
                    }

                    @Override
                    public Exam_CountCourse add(Exam_CountCourse value, Exam_CountCourse accumulator) {
                        value.setNumberExam(Long.sum(value.getNumberExam(), accumulator.getNumberExam()));
                        value.setTotalScore(Double.sum(value.getTotalScore() == null ? 0D : value.getTotalScore(), accumulator.getTotalScore()));
                        value.setTotalDuration(Double.sum(value.getTotalDuration(), accumulator.getTotalDuration()));
                        return value;
                    }

                    @Override
                    public Exam_CountCourse getResult(Exam_CountCourse accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Exam_CountCourse merge(Exam_CountCourse a, Exam_CountCourse b) {
                        return null;
                    }
                }, new WindowFunction<Exam_CountCourse, Exam_CountCourse, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Exam_CountCourse> input, Collector<Exam_CountCourse> out) throws Exception {
                        // 封装窗口时间
                        String start = HedeoerUtil.toDateTime(window.getStart());
                        String end = HedeoerUtil.toDateTime(window.getEnd());

                        // input
                        Exam_CountCourse next = input.iterator().next();
                        Long numberExam = next.getNumberExam();
                        Double avgDur = next.getTotalDuration() / numberExam;
                        Double avgScore = next.getTotalScore() / numberExam;

                        next.setAverageScore(avgScore);
                        next.setAverageDuration(avgDur);
                        next.setTs(System.currentTimeMillis());
                        next.setStt(start);
                        next.setEdt(end);

                        out.collect(next);

                    }
                });
    }

    /**
     * 学员重复考试处理
     * 并将将数据封装成bean
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<Exam_CountCourse> Json2Pojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.keyBy(e -> e.getString("course_id"))
                .process(new KeyedProcessFunction<String, JSONObject, Exam_CountCourse>() {
                    private MapState<String, JSONObject> userState;

                    // 有可能一个人当门课程在一场考试中重复考试？

                    /**
                     * String : user_id
                     * JSONObject : 该学员的当次考试信息
                     * @param parameters The configuration containing the parameters attached to the contract.
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userState = getRuntimeContext()
                                .getMapState(new MapStateDescriptor<String, JSONObject>("userState", String.class, JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<Exam_CountCourse> out) throws Exception {
                        double score = Double.parseDouble(value.getString("score"));
                        double durationSec = Double.parseDouble(value.getString("duration_sec"));
                        Long numbers = 1L;
/*
* 2> Exam_CountCourse(stt=null, edt=null, paperId=null, courseId=438, courseName=null, totalScore=58.1, totalDuration=943.0, numberExam=1, averageScore=null, averageDuration=null, ts=null)
2> Exam_CountCourse(stt=null, edt=null, paperId=null, courseId=438, courseName=null, totalScore=null, totalDuration=-943.0, numberExam=0, averageScore=null, averageDuration=null, ts=null)
* */
                        String userId = value.getString("user_id");
                        JSONObject mes = userState.get(userId);
                        userState.put(userId,value);
                        // 该次考试此人有重复考
                        if (mes != null) {
                            // 取最后一次的考试成绩
                            numbers = 0L;
                            // 用于抵消第一次的参考数据
                            Exam_CountCourse bean = Exam_CountCourse.builder()
                                    .courseId(mes.getString("course_id"))
                                    .totalDuration(-mes.getDouble("score"))
                                    .totalDuration(-mes.getDouble("duration_sec"))
                                    .numberExam(numbers)
                                    .build();
                            out.collect(bean);
                        }


                        Exam_CountCourse bean = Exam_CountCourse.builder()
                                .courseId(value.getString("course_id"))
                                .totalScore(score)
                                .totalDuration(durationSec)
                                .numberExam(numbers)
                                .build();
                        out.collect(bean);

                    }
                });

    }

    private SingleOutputStreamOperator<JSONObject> preComingObject(DataStreamSource<String> stream) {
        return stream.map(JSON::parseObject)
                // 分发水印
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getLong("ts") * 1000)
                        .withIdleness(Duration.ofSeconds(3)));
    }

    /**
     * 使用paper_id 先获得course_id,因为需要先按照course_id分组处理数据
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> addDimensionByPhoenix(SingleOutputStreamOperator<JSONObject> stream) {
        // 根据paper_id 得到 course_id
        return AsyncDataStream.unorderedWait(
                stream,
                new EduSyncFunction<JSONObject>() {
                    @Override
                    public String getTableName() {
                        return "DIM_TEST_PAPER";
                    }

                    @Override
                    public String getDimensionId(JSONObject input) {
                        return input.getString("paper_id");
                    }

                    @Override
                    public void addDim(JSONObject input, JSONObject dimensions) {
                        input.put("course_id", dimensions.getString("COURSE_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );


    }
}
