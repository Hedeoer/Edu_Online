package cn.hedeoer.app.dwd;

import cn.hedeoer.app.BaseSQLApp;
import cn.hedeoer.common.Constant;
import cn.hedeoer.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Dwd_01_UserAnswerPaper
 * Package: cn.hedeoer.app.dwd
 * Description: dwd层用户答卷事实表
 *
 * @Author hedeoer
 * @Create 2022/12/25 10:42
 */
public class Dwd_01_UserAnswerPaper extends BaseSQLApp {

    public static void main(String[] args) {
        new Dwd_01_UserAnswerPaper()
                .init(
                        7866,
                        2,
                        "Dwd_01_UserAnswerPaper",
                        Constant.KAFKA_ODS_DB_TOPIC
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment TEnv) {

        readDataFromOdsDB(TEnv,"Dwd_01_UserAnswerPaper");
//        TEnv.sqlQuery("select * from ods_db").execute().print();


        Table testExam = TEnv.sqlQuery("select " +
                "data['id'] exam_id, " +
                "data['user_id'] user_id, " +
                "data['paper_id'] paper_id, " +
                "data['score'] score, " +
                "data['duration_sec'] duration_sec, " +
                "data['submit_time'] submit_time, " +
                "ts " +
                "from ods_db " +
                "where `database`= 'edu' and `table` = 'test_exam' " +
                "and `type` = 'insert' ");
        TEnv.createTemporaryView("test_exam",testExam);
//        TEnv.sqlQuery("select * from test_exam ").execute().print();


        // paper为维度数据，不可从dwd取用
/*        Table testPaper = TEnv.sqlQuery("select " +
                "data['id'] paper_id," +
                "data['course_id'] course_id," +
                "data['paper_title'] paper_title " +
                "from ods_db " +
                "where `database`= 'edu' and `table` = 'test_paper' " +
                "and `type` = 'insert' ");
        TEnv.createTemporaryView("test_paper",testPaper);

        TEnv.sqlQuery("select * from test_paper").execute().print();*/

        TEnv.executeSql("create table edu_dwd_topic_user_answer_paper( " +
                "exam_id string," +
                "user_id string," +
                "paper_id string," +
                "score string," +
                "duration_sec string," +
                "submit_time string," +
                "ts bigint" +
                " )" + SqlUtil.getKafkaSink(Constant.EDU_DWD_TOPIC_USER_ANSWER_PAPER,"json") +"");

        testExam.executeInsert("edu_dwd_topic_user_answer_paper");

    }
}
