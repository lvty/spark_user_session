package com.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

    //定义输入数据的字段与类型schema
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo", DataTypes.StringType, true)
    ));

    //指定缓冲数据的字段与类型
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)
    ));


    private DataType dataType = DataTypes.StringType;//返回类型

    //指定是否是确定性的
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    /**
     * 初始化，可以认为是在内部自己指定一个初始的值
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,"");
    }

    /**
     * 更新操作，可看做是一个一个的将组内的字段传递进来，实现拼接的逻辑
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //缓冲中的已经拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);
        //刚刚传递进来的某一个城市信息
        String newCityInfo = input.getString(0);

        //实现去重逻辑
        if(!bufferCityInfo.contains(newCityInfo)){
            if("".equals(bufferCityInfo)){
                bufferCityInfo += newCityInfo;
            }else{
                //1:bj,2:sh
                bufferCityInfo += "," + newCityInfo;
            }
        }

        //更新操作
        buffer.update(0,bufferCityInfo);

    }

    /**
     * 合并操作，多分布式的多个节点进行合并操作，update只是局部信息
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for(String s:bufferCityInfo2.split(",")){
            if(!bufferCityInfo1.contains(s)){
                if("".equals(bufferCityInfo1)){
                    bufferCityInfo1 += s;
                }else{
                    bufferCityInfo1 += "," + s;
                }
            }
        }

        buffer1.update(0,bufferCityInfo1);

    }

    /**
     *
     * @param buffer
     * @return
     */
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
