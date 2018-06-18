package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ISessionAggrStatDAO;
import com.spark.user_session.domain.SessionAggr;
import com.spark.user_session.jdbc.JDBCHelper;

public class SessionAggrImpl implements ISessionAggrStatDAO {

    /**
     * 具体实现插入sessionAggr结果
     * @param sessionAggr
     */
    @Override
    public void insert(SessionAggr sessionAggr) {
        String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionAggr.getTaskid(),
                sessionAggr.getSessioncount(),
                sessionAggr.getVisitlength_1s_3s_ratio(),
                sessionAggr.getVisitlength_4s_6s_ratio(),
                sessionAggr.getVisitlength_7s_9s_ratio(),
                sessionAggr.getVisitlength_10s_30s_ratio(),
                sessionAggr.getVisitlength_30s_60s_ratio(),
                sessionAggr.getVisitlength_1m_3m_ratio(),
                sessionAggr.getVisitlength_3m_10m_ratio(),
                sessionAggr.getVisitlength_10m_30m_ratio(),
                sessionAggr.getVisitlength_30m_ratio(),
                sessionAggr.getSteplength_1_3_ratio(),
                sessionAggr.getSteplength_4_6_ratio(),
                sessionAggr.getSteplength_7_9_ratio(),
                sessionAggr.getSteplength_10_30_ratio(),
                sessionAggr.getSteplength_30_60_ratio(),
                sessionAggr.getSteplength_60_ratio()
        };

        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql,params);
    }

    /**
     * 获取dao
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDao(){
        return new SessionAggrImpl();
    }


}
