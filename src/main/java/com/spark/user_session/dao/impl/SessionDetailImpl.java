package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ISessionDeatilDao;
import com.spark.user_session.domain.SessionDetail;
import com.spark.user_session.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class SessionDetailImpl implements ISessionDeatilDao {

    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionDetail.getTask_id(),
                sessionDetail.getUser_id(),
                sessionDetail.getSession_id(),
                sessionDetail.getPage_id(),
                sessionDetail.getAction_time(),
                sessionDetail.getSearch_keyword(),
                sessionDetail.getClick_category_id(),
                sessionDetail.getClick_product_id(),
                sessionDetail.getOrder_category_ids(),
                sessionDetail.getOrder_product_ids(),
                sessionDetail.getPay_category_ids(),
                sessionDetail.getPay_product_ids()
        };

        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql,params);
    }

    @Override
    public void insert(List<SessionDetail> sessionDetails) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        List<Object[]> params = new ArrayList<>();
        for(SessionDetail s:sessionDetails){
            Object[] param = new Object[]{
                    s.getTask_id(),
                    s.getUser_id(),
                    s.getSession_id(),
                    s.getPage_id(),
                    s.getAction_time(),
                    s.getSearch_keyword(),
                    s.getClick_category_id(),
                    s.getClick_product_id(),
                    s.getOrder_category_ids(),
                    s.getOrder_product_ids(),
                    s.getPay_category_ids(),
                    s.getPay_product_ids()
            };
            params.add(param);
        }
        JDBCHelper.getInstance().executeBatch(sql,params);
    }

    public static ISessionDeatilDao getSessionDetailImpl(){
        return new SessionDetailImpl();
    }
}
