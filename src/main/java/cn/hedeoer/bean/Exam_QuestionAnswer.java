package cn.hedeoer.bean;

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
public class Exam_QuestionAnswer {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 问题id
    String questionId;
    // 正确答题次数
    Long correctCount;
    // 答题次数
    Long answerTimes;
    // 正确独立答题用户数
    Long correctUct ;
    // 答题独立用户数
    Long answerUct;
    // 正确答题用户占比
    Double rate;
    Long ts;
}
