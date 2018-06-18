package com.spark.product;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefix implements UDF1<String,String>{
    @Override
    public String call(String s) throws Exception {

        return s.split("_")[1];
    }
}
