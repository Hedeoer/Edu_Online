package cn.hedeoer.util;


import cn.hedeoer.common.Constant;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: JdbcUtil
 * Package: cn.hedeoer.exec.util
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/3 16:04
 */
public class JdbcUtil {

    private static Connection connection;

    public static Connection getPhoenixConnection() {
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
        return getJdbcConnection(url,driver,null,null);
    }

    private static Connection getJdbcConnection(String url,String driver,String userName,String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            connection = DriverManager.getConnection(url,userName,password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    public static Connection getMysqlConnection() {
        String url = Constant.MYSQLURL;
        String driver = Constant.MYSQLDRIVER;
        String userName = Constant.MYSQLUSERNAME;
        String password = Constant.MYSQLPASSWORD;
        return getJdbcConnection(url,driver,userName, password);
    }

    /**
     *
     * @param querySql 查询语句
     * @param conn  连接
     * @param pos   站位符
     * @param aClass 结果集 每行封装类型
     * @param isUnderlineToCamel 是否下划线转小驼峰
     * @return 查询后的结果集
     * @param <T>
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public static<T> List<T> queryConfig(String querySql, Connection conn, Object[] pos, Class<T> aClass, boolean isUnderlineToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        List<T> list = new ArrayList<T>();

        // 创建preparestatement
        PreparedStatement pt = conn.prepareStatement(querySql);
        // 为站位符赋值
        for (int i = 0; pos != null &&  i < pos.length; i++) {
            pt.setObject(i + 1,pos[i]);
        }
        // 执行sql
        ResultSet resultSet = pt.executeQuery();
        ResultSetMetaData meta = resultSet.getMetaData();
        // 封装ResultSet 为 List<TableProcess>
        while (resultSet.next()) {
            T t = aClass.newInstance();
            // 获取列名
            for (int i = 0; i < meta.getColumnCount(); i++) {
                // 获取列的别名
                String columnName = meta.getColumnLabel(i + 1);
                // 获取列值
                Object columnValue = resultSet.getObject(i + 1);

                if (isUnderlineToCamel) {
                    // 小驼 LOWER_CAMEL: 方法名, 属性名, 变量名  首字母小写, 其他单词的首字母大小   myName
                    // 大驼 UPPER_CAMEL: 类名, 接口名  所有单词的首字母   MyName
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                //t.name=value
                BeanUtils.setProperty(t,columnName,columnValue);

            }
        list.add(t);
        }
        // 返回结果
        return list;
    }


    public static JSONObject getDimFromPhoenix(Connection conn, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 预处理语句 和sql
        String sql = "select * from " + tableName + " where id=? " ;
        List<JSONObject> querySet = JdbcUtil.queryConfig(sql, conn, new String[]{id}, JSONObject.class, false);

        if (querySet.size() == 0) {
            System.out.println("查询" + tableName + "维度" + id + "缺失");
        }
        JSONObject dimension = querySet.get(0);
        return dimension;
    }

    public static void main(String[] args) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
//        Connection conn = getMysqlConnection();
        Connection conn = getPhoenixConnection();
        List<JSONObject> list = queryConfig("select * from DIM_SKU_INFO", conn, null, JSONObject.class, true);
        for (JSONObject obj : list) {
            System.out.println(obj);
        }
    }

    public static JSONObject getDim(Connection conn, Jedis client, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 从缓存读取数据
        JSONObject dim = readDimFromRedis(client,tableName,id);
        // 缓存获取到，直接返回
        if (dim == null) {
            // 缓存没有， 从phoenix获取并写入到缓存
            dim = getDimFromPhoenix(conn, tableName, id);
            writeDimToRedis(client,tableName,id,dim);
//            System.out.println("从phoenix读取维度数据 " + dim);
        }
//        System.out.println("从redis读取维度数据 " + dim);
        return dim;
    }

    private static void writeDimToRedis(Jedis client, String tableName, String id, JSONObject dim) {
        // 写维度数据到redis
        String key = tableName + ":" + id;
        // 并设置key的生命周期为2天
        client.setex(key,24 * 2 * 60 * 60,dim.toJSONString());
    }

    private static JSONObject readDimFromRedis(Jedis client, String tableName, String id) {
        String key = tableName + ":" + id;
        String dim = client.get(key);
        if (dim != null) {
            // 从redis读取维度数据，不为null，直接返回
            return JSON.parseObject(dim);
        }
        // 为null，返回null
        return null;
    }
}
