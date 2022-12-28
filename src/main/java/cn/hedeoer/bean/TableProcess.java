package cn.hedeoer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
    // 来源表
    private String sourceTable;

    // 来源操作类型
    private String sourceType;

    // 输出表
    private String sinkTable;

    // 输出类型 dwd | dim
    private String sinkType;

    // 输出字段
    private String sinkColumns;

    // 主键字段
    private String sinkPk;

    // 建表扩展
    private String sinkExtend;
    
    // op: 根据这个可以对不同的操作对表做不同的 ddl
    private String op;
}