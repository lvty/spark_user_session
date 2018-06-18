package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ISessionRandomExtractDao;
import com.spark.user_session.domain.SessionRandomExtract;
import com.spark.user_session.jdbc.JDBCHelper;

public class SessionRandomExtractDaoImpl implements ISessionRandomExtractDao {

    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionRandomExtract.getTask_id(),
                sessionRandomExtract.getSession_id(),
                sessionRandomExtract.getStart_time(),
                sessionRandomExtract.getSearch_keywords(),
                sessionRandomExtract.getClick_category_ids()
        };

        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql,params);
    }

    public static ISessionRandomExtractDao getSessionRandomExtractDaoImpl(){
        return new SessionRandomExtractDaoImpl();
    }
}
