package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.IPageConvertDao;
import com.spark.user_session.domain.PageConvert;
import com.spark.user_session.jdbc.JDBCHelper;

public class PageConvertDaoImpl implements IPageConvertDao {
    @Override
    public void insert(PageConvert pageConvert) {
        String sql = "insert into page_convert_rate values(?,?)";
        Object[] params = new Object[]{
                pageConvert.getTaskid(),
                pageConvert.getConvertRate()
        };
        JDBCHelper.getInstance().executeUpdate(sql,params);
    }

    public static IPageConvertDao getPageConvertDaoImpl(){
        return new PageConvertDaoImpl();
    }
}
