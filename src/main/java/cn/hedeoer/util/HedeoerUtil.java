package cn.hedeoer.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ClassName: HedeoerUtil
 * Package: cn.hedeoer.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/25 14:49
 */
public class HedeoerUtil {

    public static String toDate(Long ts) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        // TODO ts为s可以吗？不可以
        return simpleDateFormat.format(ts );
    }

    public static String toDateTime(long ts) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // TODO ts为s可以吗？不可以
        return simpleDateFormat.format(ts);
    }
}
