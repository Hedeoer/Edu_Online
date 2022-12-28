package cn.hedeoer.util;


import cn.hedeoer.bean.TableProcess;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName: PhoenixSink
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/4 14:26
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private DruidPooledConnection phoenixConn;
    private Jedis client;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取phoenix连接
        phoenixConn = DruidDSUtil.getPhoenixConn();
        // 获取redis client
        client = RedisUtil.getRedisClient();
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getSink() {
        return new PhoenixSink();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> data, Context ctx) throws Exception {
        JSONObject columnValues = data.f0;
        TableProcess tableProcess = data.f1;
        // 写出数据到phoenix
        writeDimToPhoenix(columnValues, tableProcess);

        // 根据维度数据中的op_type字段值，为update时删除redis中旧维度数据
        String value = columnValues.getString("op_type");
        if ("update".equals(value)) {
            //删除redis的旧缓存
            removeCacheFromRedis(tableProcess,columnValues);
        }

    }

    private void removeCacheFromRedis(TableProcess tableProcess, JSONObject dim) {
        // 获取redis的客户端
        // 删除数据 key的格式为 表名大写:id！！而配置表（tableProcess）中sinktable为小写， 需要转成大写，否则无法删除redis
        // 中 旧缓存数据
        String key = tableProcess.getSinkTable().toUpperCase() + ":" + dim.getString("id");
        client.del(key);
        System.out.println("删除redis缓存" + key);
    }

    private void writeDimToPhoenix(JSONObject columnValues, TableProcess tableProcess) throws SQLException {
        // upsert into person(id,name) values(00,'lisi')
        // 拼接sql
        StringBuilder sql = new StringBuilder();
        sql.append("upsert into ")
                .append(tableProcess.getSinkTable() + " (")
                .append(tableProcess.getSinkColumns())
                .append(") values ( ")
                .append(tableProcess.getSinkColumns().replaceAll("[^,]+","?"))
                .append(")");
        // 获取prepareStatement
        PreparedStatement ps = phoenixConn.prepareStatement(sql.toString());

        // 填充占位符
        String[] columns = tableProcess.getSinkColumns().split(",");
        for (int i = 0; i < columns.length; i++) {
            String value = columnValues.getString(columns[i]);
            ps.setString(i+1,value);
        }
        // 执行sql
        ps.execute();

        // 关闭prepareStatement对象
        ps.close();
    }

    @Override
    public void close() throws Exception {
        if (phoenixConn != null && !phoenixConn.isClosed() ) {
            // 归还连接
            phoenixConn.close();
        }
        // 归还redis的连接
        if (client !=null) {
            client.close();
        }

    }
}
