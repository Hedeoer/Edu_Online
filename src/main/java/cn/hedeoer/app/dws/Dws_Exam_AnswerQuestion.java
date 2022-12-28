package cn.hedeoer.app.dws;

import cn.hedeoer.app.BaseStreamApp;
import cn.hedeoer.bean.Exam_QuestionAnswer;
import cn.hedeoer.common.Constant;
import cn.hedeoer.util.FlinkSinkUtil;
import cn.hedeoer.util.HedeoerUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: Dws_Exam_AnswerQuestion
 * Package: cn.hedeoer.app.dws
 * Description:
 * 统计当日 每个题目：
 * 正确答题次数
 * 答题次数
 * 正确率
 * 正确答题独立用户数
 * 答题独立用户数
 * 正确答题用户占比
 *
 * @Author hedeoer
 * @Create 2022/12/25 13:10
 */
public class Dws_Exam_AnswerQuestion extends BaseStreamApp {
    public static void main(String[] args) {
        new Dws_Exam_AnswerQuestion()
                .init(
                        8989,
                        2,
                        "Dws_Exam_AnswerQuestion",
                        Constant.EDU_DWD_TOPIC_USER_ANSWER_QUESTION
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 为数据添加水印 json --》 JSONObject
        SingleOutputStreamOperator<JSONObject> eventStream = addWaterMarkAndComingObject(stream);
//        eventStream.print();
        // 2. bean
        SingleOutputStreamOperator<Exam_QuestionAnswer> beanStream = ComingBean(eventStream);
//        beanStream.print();
        // 3. 分组 开窗 聚合
        SingleOutputStreamOperator<Exam_QuestionAnswer> resultStream = groupAndAgg(beanStream);
//        resultStream.print();

        // 4. 写出结果到clickhouse
        resultStream.addSink(FlinkSinkUtil.getClickhouseSink("dws_exam_question_window",Exam_QuestionAnswer.class));


    }

    private SingleOutputStreamOperator<Exam_QuestionAnswer> groupAndAgg(SingleOutputStreamOperator<Exam_QuestionAnswer> beanStream) {
        return beanStream.keyBy( Exam_QuestionAnswer :: getQuestionId)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .reduce(new ReduceFunction<Exam_QuestionAnswer>() {
                    @Override
                    public Exam_QuestionAnswer reduce(Exam_QuestionAnswer value1, Exam_QuestionAnswer value2) throws Exception {
                        value1.setCorrectCount(value1.getCorrectCount() + value2.getCorrectCount());
                        value1.setAnswerTimes(value1.getAnswerTimes() + value2.getAnswerTimes());
                        value1.setCorrectUct(value1.getCorrectUct() + value2.getCorrectUct());
                        value1.setAnswerUct(value1.getAnswerUct() + value2.getAnswerUct());


                        return value1;
                    }
                }, new ProcessWindowFunction<Exam_QuestionAnswer, Exam_QuestionAnswer, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<Exam_QuestionAnswer> elements,
                                        Collector<Exam_QuestionAnswer> out) throws Exception {
                        // 封装窗口时间
                        String start = HedeoerUtil.toDateTime(context.window().getStart());
                        String end = HedeoerUtil.toDateTime(context.window().getEnd());

                        // 计算正确答题的用户比列
                        Exam_QuestionAnswer reduceResult = elements.iterator().next();
                        Double rate =  reduceResult.getCorrectUct() / (reduceResult.getAnswerUct() * 1.0D);

                        // 封装聚合结果系统时间
                        long ts = System.currentTimeMillis();

                        reduceResult.setStt(start);
                        reduceResult.setEdt(end);
                        reduceResult.setRate(rate);
                        reduceResult.setTs(ts);


                        out.collect(reduceResult);

                    }
                });
    }

    /**
     * 将每一条答题数据封装成bean，供后面分组聚合
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<Exam_QuestionAnswer> ComingBean(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.keyBy(o ->  o.getString("user_id"))

                .map(new RichMapFunction<JSONObject, Exam_QuestionAnswer>() {

                    private ValueState<String> answerLastIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        answerLastIdState = getRuntimeContext().getState(new ValueStateDescriptor<String>("answerLastId", String.class));

                    }


                    @Override
                    public Exam_QuestionAnswer map(JSONObject value) throws Exception {

                        String questionId = value.getString("question_id");
                        Long ts = value.getLong("ts") ;
                        Long correctCount = 0L;
                        Long answerTimes = 1L;
                        Long correctUct = 0L;
                        Long answertUct = 0L;
                        String today = HedeoerUtil.toDate(ts);
                        String isCorrect = value.getString("is_correct");

                        // 正确答题次数 + 1
                        if ("1".equals(isCorrect)) {
                            correctCount = 1L;
                        }

                        // 本次回答的问题和上次的不一样
                        if (!questionId.equals(answerLastIdState.value())) {
                            // 答题用户 + 1
                            answertUct = 1L;
                            answerLastIdState.update(today);
                            // 回答问题正确
                            if ("1".equals(isCorrect)) {
                                // 正确答题用户 + 1
                                correctUct = 1L;

                            }

                        }

                        return new Exam_QuestionAnswer("","",questionId,correctCount,answerTimes,correctUct,answertUct,0.0D,ts);
                    }
                });

    }

    /**
     * 将答题数据添加水印
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> addWaterMarkAndComingObject(DataStreamSource<String> stream) {
        return stream.map((MapFunction<String, JSONObject>) JSONObject::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getLong("ts") * 1000)
                        .withIdleness(Duration.ofSeconds(3)));
    }
}
