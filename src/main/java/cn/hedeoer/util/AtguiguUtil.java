package cn.hedeoer.util;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * ClassName: AtguiguUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/5 14:14
 */
public class AtguiguUtil {

    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }

    public static Set<String> analyzeWord(String searches) {

        Set<String> set = new HashSet<>();
        StringReader input = new StringReader(searches);
        IKSegmenter ikSegmenter = new IKSegmenter(input, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                set.add(word);
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return set;
    }

    public static String toDateTime(long ts) {
        return  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }

    public static long toTs(String date) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
