package com.spark.user_session.dao.impl;

import com.spark.ad.model.AdClickTrendQueryResult;
import com.spark.user_session.dao.IAdClickTrendDao;
import com.spark.user_session.domain.AdClickTrend;
import com.spark.user_session.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdClickTrendDaoImpl implements IAdClickTrendDao {
    @Override
    public void updateBatch(List<AdClickTrend> list) {
        //有就更新，没有就插入
        ArrayList<AdClickTrend> updates = new ArrayList<>();
        ArrayList<AdClickTrend> inserts = new ArrayList<>();

        String selectsql = "select count(*) from ad_click_trend " +
                "where date = ? and hour = ? and minute =? and ad_id = ?";
        for (AdClickTrend a : list) {
            final AdClickTrendQueryResult adClickTrendQueryResult
                    = new AdClickTrendQueryResult();
            Object[] objects = {
                    a.getDate(),
                    a.getHour(),
                    a.getMiute(),
                    a.getAdid()
            };
            JDBCHelper.getInstance().executeQuery(selectsql, objects, new JDBCHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()){
                        adClickTrendQueryResult.setCount(rs.getInt(1));
                    }
                }
            });

            if(adClickTrendQueryResult.getCount() > 0){
                updates.add(a);
            }else{
                inserts.add(a);
            }
        }

        String updateSql = "update ad_click_trend set click_count = ? " +
                "where date = ? and hour = ? and minute =? and ad_id = ?";

        ArrayList<Object[]> updateparams = new ArrayList<>();
        for(AdClickTrend a:updates){
            updateparams.add(new Object[]{
                    a.getDate(),
                    a.getHour(),
                    a.getMiute(),
                    a.getAdid()
            });
        }

        JDBCHelper.getInstance().executeBatch(updateSql,updateparams);


        String insertSql = "insert into ad_click_trend values(?,?,?,?,?)";

        ArrayList<Object[]> insertparams = new ArrayList<>();
        for(AdClickTrend a:inserts){
            updateparams.add(new Object[]{
                    a.getHour(),
                    a.getDate(),
                    a.getAdid(),
                    a.getMiute(),
                    a.getClick_count()
            });
        }

        JDBCHelper.getInstance().executeBatch(insertSql,insertparams);
    }

    public static IAdClickTrendDao getAdClickTrendDaoImpl(){
        return new AdClickTrendDaoImpl();
    }
}
