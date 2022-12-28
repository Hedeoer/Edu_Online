package cn.hedeoer.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DimThreadPoolUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/14 15:30
 */
public class DimThreadPoolUtil {

    public static ThreadPoolExecutor getThreadPool() {

        return  new ThreadPoolExecutor(
                300, 500,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>()
        );
    }
}
