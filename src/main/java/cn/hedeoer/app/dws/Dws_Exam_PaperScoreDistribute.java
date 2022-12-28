package cn.hedeoer.app.dws;

import cn.hedeoer.app.BaseStreamApp;
import cn.hedeoer.bean.Exam_PaperScore;
import cn.hedeoer.common.Constant;
import cn.hedeoer.util.FlinkSinkUtil;
import cn.hedeoer.util.HedeoerUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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

/**
 * ClassName: Dws_Exam_PaperScoreDistribute
 * Package: cn.hedeoer.app.dws
 * Description:
 * 各试卷成绩分布
 *
 * @Author hedeoer
 * @Create 2022/12/25 16:32
 */
public class Dws_Exam_PaperScoreDistribute extends BaseStreamApp {

    public static void main(String[] args) {
        new Dws_Exam_PaperScoreDistribute()
                .init(
                        9899,
                        2,
                        "Dws_Exam_PaperScoreDistribute",
                        Constant.EDU_DWD_TOPIC_USER_ANSWER_PAPER
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // json 转为 pojo
        SingleOutputStreamOperator<Exam_PaperScore> resultStream = stream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getLong("ts") * 1000)
                        .withIdleness(Duration.ofSeconds(3)))
                .keyBy(e -> e.getString("paper_id"))
                .process(new KeyedProcessFunction<String, JSONObject, Exam_PaperScore>() {

                             private ValueState<Long> greatestUpState;
                             private ValueState<Long> betterUpState;
                             private ValueState<Long> cheerUpState;

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 cheerUpState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cheerUP", Long.class));
                                 betterUpState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("betterUP", Long.class));
                                 greatestUpState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("greatestUP", Long.class));
                             }

                             @Override
                             public void processElement(JSONObject value,
                                                        Context ctx,
                                                        Collector<Exam_PaperScore> out) throws Exception {


                                 String paperId = value.getString("paper_id");
                                 Long ts = value.getLong("ts");
                                 float score = Float.parseFloat(value.getString("score"));
                                 Long cheerUp = 0L;
                                 Long betterUp = 0L;
                                 Long superUp = 0L;

                                 // 试卷分数段
                                 if (score < 75) {
                                     cheerUp = 1L;
                                     cheerUpState.update(cheerUp);
                                 } else if (score < 95) {
                                     betterUp = 1L;
                                     betterUpState.update(betterUp);
                                 } else {
                                     superUp = 1L;
                                     greatestUpState.update(superUp);
                                 }

                                 if (score >= 0) {
                                     out.collect(new Exam_PaperScore("", "", paperId, cheerUp, betterUp, superUp, ts));
                                 }


                             }
                         }
                )
                .keyBy(Exam_PaperScore::getPaperId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .reduce(new ReduceFunction<Exam_PaperScore>() {
                    @Override
                    public Exam_PaperScore reduce(Exam_PaperScore value1, Exam_PaperScore value2) throws Exception {
                        value1.setBetterUp(value1.getBetterUp() + value2.getBetterUp());
                        value1.setCheerUp(value1.getCheerUp() + value2.getCheerUp());
                        value1.setSuperUp(value1.getSuperUp() + value2.getSuperUp());
                        return value1;
                    }
                }, new ProcessWindowFunction<Exam_PaperScore, Exam_PaperScore, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<Exam_PaperScore> elements,
                                        Collector<Exam_PaperScore> out) throws Exception {

                        // 封装窗口时间
                        String start = HedeoerUtil.toDateTime(context.window().getStart());
                        String end = HedeoerUtil.toDateTime(context.window().getEnd());

                        long ts = System.currentTimeMillis();
                        Exam_PaperScore next = elements.iterator().next();
                        next.setEdt(end);
                        next.setStt(start);
                        next.setTs(ts);

                        out.collect(next);

                    }
                });
        
        resultStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_exam_paper_score_window",Exam_PaperScore.class));


    }
}
