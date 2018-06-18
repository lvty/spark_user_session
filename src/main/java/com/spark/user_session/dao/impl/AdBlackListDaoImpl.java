package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.IAdBlackListDao;
import com.spark.user_session.domain.AdBlackList;
import com.spark.user_session.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告黑名单dao实现类
 */
public class AdBlackListDaoImpl implements IAdBlackListDao{

    @Override
    public void insertBatch(List<AdBlackList> adBlackLists) {
        String sql = "insert into ad_blacklist values(?)";
        ArrayList<Object[]> paramsList = new ArrayList<>();
        for(AdBlackList a:adBlackLists){
            Object[] params = {a.getUserid()};
            paramsList.add(params);
        }
        JDBCHelper.getInstance().executeBatch(sql,paramsList);
    }

    @Override
    public List<AdBlackList> findAll() {

        String sql = "select * from ad_blacklist";
        final ArrayList<AdBlackList> adBlackLists = new ArrayList<>();
        JDBCHelper.getInstance().executeQuery(sql, null, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    Long userid = Long.valueOf(String.valueOf(rs.getObject(1)));
                    AdBlackList adBlackList = new AdBlackList();
                    adBlackList.setUserid(userid);
                    adBlackLists.add(adBlackList);
                }
            }
        });
        return adBlackLists;
    }

    public static IAdBlackListDao getAdBlackListDaoImpl(){
        return new AdBlackListDaoImpl();
    }
}
