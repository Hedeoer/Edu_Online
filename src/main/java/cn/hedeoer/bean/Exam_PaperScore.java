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
public class Exam_PaperScore {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 试卷id
    String paperId;
    // 0-75
    Long cheerUp;
    // 75 - 95
    Long betterUp;
    // 95-100
    Long superUp ;
    Long ts;
}
