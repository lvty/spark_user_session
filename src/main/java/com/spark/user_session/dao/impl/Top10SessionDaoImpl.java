package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ITop10SessionDao;
import com.spark.user_session.domain.Top10Session;
import com.spark.user_session.jdbc.JDBCHelper;

public class Top10SessionDaoImpl implements ITop10SessionDao {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_category_session values(?,?,?,?)";
        Object[] params = new Object[]{
                top10Session.getTask_id(),
                top10Session.getCategory_id(),
                top10Session.getSessionid(),
                top10Session.getClick_count()
        };

        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql,params);
    }

    public static ITop10SessionDao getTop10SessionDaoImpl(){
        return new Top10SessionDaoImpl();
    }
}
