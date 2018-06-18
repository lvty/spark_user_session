package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.IAreaTop3ProductDao;
import com.spark.user_session.domain.AreaTop3Product;
import com.spark.user_session.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class AreaTop3ProductDaoImpl implements IAreaTop3ProductDao {
    @Override
    public void insertBatch(List<AreaTop3Product> list) {
        String sql = "insert into area_top3_product values (?,?,?,?,?,?,?,?)";
        List<Object[]> paramsList = new ArrayList<>();
        for(AreaTop3Product a:list){
            Object[] params = new Object[8];
            params[0] = a.getTaskid();
            params[1] = a.getArea();
            params[2] = a.getAreaLevel();
            params[3] = a.getProductid();
            params[4] = a.getCityInfos();
            params[5] = a.getClickCount();
            params[6] = a.getProductName();
            params[7] = a.getProductStatus();
            paramsList.add(params);
        }

        JDBCHelper.getInstance().executeBatch(sql,paramsList);
    }
    public static IAreaTop3ProductDao getAreaTop3ProductDaoImpl(){
        return new AreaTop3ProductDaoImpl();
    }
}
