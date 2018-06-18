package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.IAdProvinceTop3Dao;
import com.spark.user_session.domain.AdProvinceTop3;
import com.spark.user_session.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class AdProvinceTop3DaoImpl implements IAdProvinceTop3Dao {
    @Override
    public void updateBatch(List<AdProvinceTop3> list) {
        //先做一次去重(date_province)
        ArrayList<String> dateProvinces = new ArrayList<>();
        for (AdProvinceTop3 a:list){
            String date = a.getDate();
            String province = a.getProvince();
            String key = date + "_" + province;
            if(!dateProvinces.contains(key)){
                dateProvinces.add(key);
            }
        }

        //根据去重后的date和province进行批量删除操作
        String deletesql = "delete from ad_province_top3 where date =? and province = ? ";

        ArrayList<Object[]> params = new ArrayList<>();
        for(String s:dateProvinces){
            String[] split = s.split("_");
            String date = split[0];
            String province = split[1];
            Object[] param = {
                    date, province
            };
            params.add(param);
        }

        JDBCHelper.getInstance().executeBatch(deletesql,params);

        //批量插入传入进来的所有数据
        String insertSql = "insert into ad_province_top3 values(?,?,?,?)";
        ArrayList<Object[]> params1 = new ArrayList<>();
        for(AdProvinceTop3 a:list){
            params1.add(new Object[]{
                    a.getDate(),
                    a.getProvince(),
                    a.getAdid(),
                    a.getClickcount()
            });
        }

        JDBCHelper.getInstance().executeBatch(insertSql,params1);

    }

    public static IAdProvinceTop3Dao getAdProvinceTop3DaoImpl(){
        return new AdProvinceTop3DaoImpl();
    }
}
