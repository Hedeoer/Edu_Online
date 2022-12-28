package cn.hedeoer.app.dwd;

import cn.hedeoer.app.BaseSQLApp;
import cn.hedeoer.common.Constant;
import cn.hedeoer.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Dwd_02_UserAnswerQuestion
 * Package: cn.hedeoer.app.dwd
 * Description: dwd用户答题事实表
 *
 * @Author hedeoer
 * @Create 2022/12/25 12:05
 */
public class Dwd_02_UserAnswerQuestion extends BaseSQLApp {

    public static void main(String[] args) {
        new Dwd_02_UserAnswerQuestion()
                .init(
                        8943,
                        2,
                        "Dwd_02_UserAnswerQuestion",
                        Constant.KAFKA_ODS_DB_TOPIC
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment TEnv) {
        readDataFromOdsDB(TEnv,"Dwd_02_UserAnswerQuestion");

        Table answerQuestion = TEnv.sqlQuery("select " +
                "data['exam_id'] exam_id," +
                "data['paper_id'] paper_id," +
                "data['question_id'] question_id," +
                "data['user_id'] user_id," +
                "data['answer'] answer," +
                "data['is_correct'] is_correct," +
                "data['score'] score," +
                "data['create_time'] create_time," +
                "ts " +
                "from ods_db " +
                "where `database`= 'edu' and `table` = 'test_exam_question' " +
                "and `type` = 'insert' ");
        TEnv.createTemporaryView("user_answer_question",answerQuestion);

//        TEnv.sqlQuery("select * from user_answer_question").execute().print();

        TEnv.executeSql("create table edu_dwd_topic_user_answer_question( " +
                "    exam_id     string ,\n" +
                "    paper_id    string ,\n" +
                "    question_id string ,\n" +
                "    user_id     string ,\n" +
                "    answer      string ,\n" +
                "    is_correct  string ,\n" +
                "    score       string ,\n" +
                "    create_time string ,\n" +
                "    ts          bigint" +
                " )" + SqlUtil.getKafkaSink(Constant.EDU_DWD_TOPIC_USER_ANSWER_QUESTION,"json") +"");

        answerQuestion.executeInsert("edu_dwd_topic_user_answer_question");

    }
}
