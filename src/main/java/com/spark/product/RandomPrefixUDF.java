package com.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * 给某字段加上随机字段
 */
public class RandomPrefixUDF implements UDF2<String,Integer,String>{

    @Override
    public String call(String val, Integer num) throws Exception {

        int randomNumber = new Random().nextInt(num);
        return randomNumber + "_" + val;
    }
}
