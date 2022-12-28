package cn.hedeoer.app.dim;

import cn.hedeoer.app.BaseStreamApp;
import cn.hedeoer.bean.TableProcess;
import cn.hedeoer.common.Constant;
import cn.hedeoer.util.FlinkSinkUtil;
import cn.hedeoer.util.JdbcUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * ClassName: DimApp
 * Package: cn.hedeoer.app.dim
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/24 12:21
 */
@Slf4j
public class DimApp extends BaseStreamApp {

    public static void main(String[] args) {
        new DimApp()
                .init(
                        4545,
                        2,
                        "DimApp",
                        Constant.KAFKA_ODS_DB_TOPIC
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
//        etledStream.print();
        // flinkCDC读取配置
        SingleOutputStreamOperator<TableProcess> configStream = readTableProcess(env);
//        configStream.print();

        // 动态向phoenix中建维度表
        createOrDropTable(configStream);

        // connect 数据流和配置流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> configuredStream = connectDataAndTpConfig(etledStream, configStream);
//        configuredStream.print();

        // 按照配置删除冗余数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = removeNotAvailableKey(configuredStream);
//        resultStream.print();

        // 向维度表中出入数据（向phoenix写）
        writeToPhoenix(resultStream);


    }



    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        stream.addSink(FlinkSinkUtil.getPhoenixSink());
    }

    //Tuple2<JSONObject, TableProcess>:
    // ({"spu_name":"华为智慧屏 4K全面屏智能电视机","tm_id":3,"description":"华为智慧屏 4K全面屏智能电视机","id":12,"op_type":"insert","category3_id":86},
    // TableProcess(sourceTable=spu_info, sourceType=ALL, sinkTable=dim_spu_info, sinkType=dim, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkPk=id, sinkExtend=SALT_BUCKETS = 3, op=r))
    //
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> removeNotAvailableKey(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {
        return connectedStream.map(new MapFunction<Tuple2<JSONObject, TableProcess>,Tuple2<JSONObject, TableProcess> >() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> tuple2Data) throws Exception {
                JSONObject tableData = tuple2Data.f0;
                TableProcess config = tuple2Data.f1;
                List<String> needColumns = Arrays.asList(config.getSinkColumns().split(","));
                Set<String> dataColumns = tableData.keySet();
                // TODO  op_type 的作用
                dataColumns.removeIf(d -> !needColumns.contains(d) &&  !"op_type".equals(d) );

                return tuple2Data;
            }
        });

    }
/*
ods_db数据：
{
    "database":"gmall2022",
    "table":"user_info",
    "type":"insert",
    "ts":1669957011,
    "xid":478,
    "xoffset":31,
    "data":{
        "id":32,
        "login_name":"0tyl9j8",
        "nick_name":null,
        "passwd":null,
        "name":null,
        "phone_num":"13384232821",
        "email":"0tyl9j8@sina.com",
        "head_img":null,
        "user_level":"1",
        "birthday":"1996-02-02",
        "gender":null,
        "create_time":"2022-12-02 04:56:50",
        "operate_time":null,
        "status":null
    }
配置流数据：

TableProcess(
    sourceTable=user_info,
    sourceType=ALL,
    sinkTable=dim_user_info,
    sinkType=dim,
    sinkColumns=id,
    login_name,name,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    sinkPk=id,
    sinkExtend= SALT_BUCKETS = 3,
    op=u)

 */

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectDataAndTpConfig(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        // tpStream做成广播流
        MapStateDescriptor<String, TableProcess> brState = new MapStateDescriptor<>("br", String.class, TableProcess.class);
        BroadcastStream<TableProcess> brStream = tpStream.broadcast(brState);

        // connect dataStream 封装成二元元组
        // 返回连接后的流
        return dataStream.connect(brStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(JSONObject data, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 从广播状态中读取配置（map）
                        String dataTable = data.getString("table");
                        String key = dataTable + ":" + "ALL";
                        ReadOnlyBroadcastState<String, TableProcess> br = ctx.getBroadcastState(brState);
                        // 当广播状态中由该维度表的配置时才将数据写出
                        if (br.get(key) != null) {
                            JSONObject tableData = data.getJSONObject("data");
                            // 同时在数据中加入本条数据从mysql同步到kafka时的类型
                            tableData.put("op_type", data.getString("type"));
                            out.collect(Tuple2.of(tableData, br.get(key)));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(brState);
                        String sourceTable = value.getSourceTable();
                        String sourceType = value.getSourceType();
                        String key = sourceTable + ":" + sourceType;

                        //将组合的key和配置项tableprocess放入广播状态
                        // 可能是需要删除一些配置，具体分析
                        if ("d".equals(value.getOp())) {
                            broadcastState.remove(key);
                        } else {
                            broadcastState.put(key, value);
                        }

                    }
                });

    }


    private void createOrDropTable(SingleOutputStreamOperator<TableProcess> tpStream) {
        tpStream.filter(t -> "dim".equals(t.getSinkType()))
                .map(new RichMapFunction<TableProcess, TableProcess>() {

                    private Connection phoenixconnection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 1 获得连接
                        phoenixconnection = JdbcUtil.getPhoenixConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        // 3. 关闭连接
                        if (phoenixconnection != null) {
                            phoenixconnection.close();
                        }

                    }

                    @Override
                    public TableProcess map(TableProcess value) throws Exception {
                        // 2. 和phoenix建立连接
                        String op = value.getOp();
                        if ("c".equals(op) || "r".equals(op)) {
                            createTable(value);
                        }else if("d".equals(op)){
                            deleteTable(value);
                        }else{
                            deleteTable(value);
                            createTable(value);
                        }
                        return  value;
                    }

                    private void deleteTable(TableProcess tp) throws SQLException {
                        // 1. sql
                        // TODO 是sinktable？
                        String dropSql = "drop table if exists " + tp.getSinkTable();

                        // 3. 执行
                        System.out.println("删除的tableSQl：" +dropSql );
                        execSQL(dropSql);
                    }



                    private void createTable(TableProcess tp) throws SQLException {
                        // create table if not exists person(id varchar , name varchar, constraint pk primary  key (id))SALT_BUCKETS = 4
                        // create table if not exists person(id varchar , name varchar, constraint pk primary  key (id))
                        // 1. sql
                        StringBuilder bu = new StringBuilder();
                        bu.append("create table if not exists ")
                                .append(tp.getSinkTable()+ "(")
                                .append(tp.getSinkColumns().replaceAll("[^,]+","$0 varchar"))
                                .append(",constraint pk primary key (" + (tp.getSinkPk() == null? "id": tp.getSinkPk()))
                                .append("))")
                                .append(tp.getSinkExtend() == null? "": tp.getSinkExtend());

                        System.out.println("建表sql为：" + bu);

                        execSQL(bu.toString());
                    }

                    private void execSQL(String sql) throws SQLException {
                        if(phoenixconnection == null){
                            phoenixconnection = JdbcUtil.getPhoenixConnection();
                        }
                        // 2. 获得预编译对象
                        PreparedStatement pre = phoenixconnection.prepareStatement(sql);
                        // 3. 执行
                        pre.execute();
                        pre.close();


                    }
                });
    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {

        Properties pros = new Properties();
        pros.setProperty("useSSL","false");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop162")
                .port(3306)
                .databaseList("edu_config") // set captured database
                .tableList("edu_config.table_process") // set captured table
                .username("root")
                .password("aaaaaa")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .jdbcProperties(pros)
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");

                        TableProcess tp;

                        if (!"d".equals(op)) {
                            // TODO tp的op字段位赋值
                            tp = JSON.parseObject(obj.getString("after"), TableProcess.class);
                        } else {
                            tp = JSON.parseObject(obj.getString("before"), TableProcess.class);
                        }
                        tp.setOp(op);
                        return tp;

                    }
                });

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {

                        try {
                            JSONObject obj = JSON.parseObject(value);
                            String database = obj.getString("database");
                            String type = obj.getString("type");
                            String data = obj.getString("data");


                            return "edu".equals(database)
                                    && ("insert".equals(type) ||
                                    "update".equals(type)
                                    || "bootstrap-insert".equals(type) )
                                    && data != null
                                    && data.length() > 2;
                        } catch (Exception e) {
                            log.warn("数据格式有误，不是json: " + value);
                            return false;
                        }
                    }
                })
                .map(m -> JSON.parseObject(m.replaceAll("bootstrap-", "")));
    }
}
