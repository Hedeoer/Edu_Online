package cn.hedeoer.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * ClassName: RedisUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/13 19:07
 */
public class RedisUtil {

    private static JedisPool pool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(90);
        config.setMinIdle(10);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        pool = new JedisPool(config, "hadoop162", 6379);
    }

    public static Jedis getRedisClient() {
        Jedis resource = pool.getResource();
        resource.select(3);
        return resource;
    }
}
