package cn.hedeoer.function;

import cn.hedeoer.util.DimThreadPoolUtil;
import cn.hedeoer.util.JdbcUtil;
import cn.hedeoer.util.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ClassName: EduSyncFunction
 * Package: cn.hedeoer.function
 * Description:
 * 自定义异步实现类 （利用多线程技术）
 * @Author hedeoer
 * @Create 2022/12/25 19:52
 */
public abstract class EduSyncFunction<T> extends RichAsyncFunction<T,T> {

    private ThreadPoolExecutor threadPool;

    public abstract String getTableName();
    public abstract String getDimensionId(T input);

    public abstract  void addDim(T input, JSONObject dimensions);

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建线程池
        threadPool = DimThreadPoolUtil.getThreadPool();
    }

    /**
     *
     * @param input element coming from an upstream task
     * @param resultFuture to be completed with the result data
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // 向线程池池提交任务
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 获取redis链接和phoenix链接
                    Jedis redisClient = RedisUtil.getRedisClient();
                    Connection phoenixConnection = JdbcUtil.getPhoenixConnection();
                    // 根据维度表民和维度id获得维度数据
                    JSONObject dimensions = JdbcUtil.getDimFromPhoenix(phoenixConnection, getTableName(), getDimensionId(input));
                    addDim(input,dimensions);
                    resultFuture.complete(Collections.singletonList(input));

                    redisClient.close();
                    phoenixConnection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });

    }
}
