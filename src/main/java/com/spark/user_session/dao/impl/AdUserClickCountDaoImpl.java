package com.spark.user_session.dao.impl;

import com.spark.ad.model.AdUserClickCountQueryResult;
import com.spark.user_session.dao.IAdUserClickCountDao;
import com.spark.user_session.domain.AdUserClickCount;
import com.spark.user_session.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdUserClickCountDaoImpl implements IAdUserClickCountDao {
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCount) {
        JDBCHelper instance = JDBCHelper.getInstance();

        ArrayList<AdUserClickCount> inserts = new ArrayList<>(); //需要插入的
        ArrayList<AdUserClickCount> updates = new ArrayList<>(); //需要更新的

        String sql = "select count(*) from ad_user_click_count where date = ? and user_id = ? and ad_id = ? ";

        Object[] params = null;
        //遍历
        for (AdUserClickCount a : adUserClickCount) {
            //实例查询结果的model
            final AdUserClickCountQueryResult adUserClickCountQueryResult = new AdUserClickCountQueryResult();

            params = new Object[]{
                    a.getDate(),
                    a.getUserid(),
                    a.getAdid()
            };

            instance.executeQuery(sql, params, new JDBCHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int cnt = rs.getInt(1);
                        adUserClickCountQueryResult.setCnt(cnt);
                    }
                }
            });

            if (adUserClickCountQueryResult.getCnt() > 0) {
                updates.add(a);//要执行更新操作
            } else {
                inserts.add(a);//要执行插入操作
            }
        }

        String insertSql = "insert into ad_user_click_count values(?,?,?,?)";
        ArrayList<Object[]> insertParams = new ArrayList<>();
        for (AdUserClickCount a : inserts) {
            Object[] param = {
                    a.getDate(),
                    a.getUserid(),
                    a.getAdid(),
                    a.getClickcnts()
            };

            insertParams.add(param);
        }

        instance.executeBatch(insertSql, insertParams);

        String updateSql = "update ad_user_click_count set click_count = click_count + ? " +
                "where date = ? and user_id = ? and ad_id = ? ";
        ArrayList<Object[]> updateParams = new ArrayList<>();
        for (AdUserClickCount a : inserts) {
            Object[] param = {
                    a.getClickcnts(),
                    a.getDate(),
                    a.getUserid(),
                    a.getAdid()
            };

            updateParams.add(param);
        }

        instance.executeBatch(updateSql, updateParams);


    }

    @Override
    public int findClickCountByMultiKeys(String date, long userid, long adid) {
        String sql = "select click_count from ad_user_click_count where date = ? and user_id = ? and ad_id = ?";
        Object[] param = new Object[]{date, userid, adid};
        final AdUserClickCountQueryResult a = new AdUserClickCountQueryResult();
        JDBCHelper.getInstance().executeQuery(sql, param, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    a.setClickcnt(rs.getInt(1));
                }
            }
        });

        return a.getClickcnt();

    }

    public static IAdUserClickCountDao getAdUserClickCountDao() {
        return new AdUserClickCountDaoImpl();
    }
}
