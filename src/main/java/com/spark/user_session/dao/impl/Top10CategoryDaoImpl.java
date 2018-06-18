package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ITop10Category;
import com.spark.user_session.domain.Top10Category;
import com.spark.user_session.jdbc.JDBCHelper;

public class Top10CategoryDaoImpl implements ITop10Category {
    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[]{
                top10Category.getTaskid(),
                top10Category.getCategoryid(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()
        };
        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql,params);
    }

    public static ITop10Category getTop10CategoryDaoImpl(){
        return new Top10CategoryDaoImpl();
    }
}
