package cn.hedeoer.bean;

import cn.hedeoer.annotation.annotation.NoSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: Video_PlayBean
 * Package: cn.hedeoer.bean
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/26 21:56
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Video_PlayBean {

    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 章节id
    String chapterId;
    //
    String chapterName;
    //
    @NoSink
    String videoId;
    //
    Long numberPlayback;
    //
    Long durationPlayback;
    //
    Long views;
    //
    Double durationCapita;

    Long ts;
}
