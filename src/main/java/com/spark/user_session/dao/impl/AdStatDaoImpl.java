package com.spark.user_session.dao.impl;

import com.spark.ad.model.AdStatQueryResult;
import com.spark.user_session.dao.IAdStatDao;
import com.spark.user_session.domain.AdStat;
import com.spark.user_session.jdbc.JDBCHelper;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdStatDaoImpl implements IAdStatDao {
    @Override
    public void updateBatch(List<AdStat> list) {
        //区分执行
        List<AdStat> inserts = new ArrayList<>();
        List<AdStat> updates = new ArrayList<>();

        String selectsql = "select count(*) from ad_stat where date = ? " +
                "and province =? and city = ? and ad_id = ? ";

        for(AdStat a:list){
            final AdStatQueryResult adRes = new AdStatQueryResult();
            Object[] param = {
                    a.getDate(),
                    a.getProvince(),
                    a.getCity(),
                    a.getAd_id()
            };

            JDBCHelper.getInstance().executeQuery(selectsql, param, new JDBCHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()){
                        adRes.setCount(rs.getInt(1));
                    }
                }
            });

            long count = adRes.getCount();
            if(count > 0){
                updates.add(a);
            }else{
                inserts.add(a);
            }
        }

        String updatesql = "update ad_stat set click_count = ? where date = ? " +
                "and province =? and city = ? and ad_id = ? ";

        ArrayList<Object[]> updateparams = new ArrayList<>();
        for(AdStat a:list){
            Object[] param = {
                    a.getClick_count(),
                    a.getDate(),
                    a.getProvince(),
                    a.getCity(),
                    a.getAd_id(),

            };
            updateparams.add(param);
        }

        JDBCHelper.getInstance().executeBatch(updatesql,updateparams);


        String insertsql ="insert into ad_stat values(?,?,?,?,?)";
        ArrayList<Object[]> insertparams = new ArrayList<>();
        for(AdStat a:list){
            Object[] param = {
                    a.getDate(),
                    a.getProvince(),
                    a.getCity(),
                    a.getAd_id(),
                    a.getClick_count()
            };
            insertparams.add(param);
        }
        JDBCHelper.getInstance().executeBatch(insertsql,insertparams);

    }

    public static IAdStatDao getAdStatDaoImpl(){
        return new AdStatDaoImpl();
    }
}
