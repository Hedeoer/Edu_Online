package cn.hedeoer.bean;

import cn.hedeoer.annotation.annotation.NoSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: Exam_QuestionAnswer
 * Package: cn.hedeoer.bean
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/25 13:37
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Exam_CountCourse {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 试卷id
    @NoSink
    String paperId;
    // 课程id
    String courseId;
    // 课程名字
    String courseName;
    // 总分
    @NoSink
    Double totalScore;
    // 总时长
    @NoSink
    Double totalDuration;
    // 考试人数
    Long numberExam;
    // 平均分
    Double averageScore;
    // 平均时长
    Double averageDuration ;
    Long ts;
}
