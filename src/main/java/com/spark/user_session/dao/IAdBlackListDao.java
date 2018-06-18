package com.spark.user_session.dao;

import com.spark.user_session.domain.AdBlackList;

import java.util.List;

public interface IAdBlackListDao {
    void insertBatch(List<AdBlackList> adBlackLists);
    //查询所有的黑名单用户
    List<AdBlackList> findAll();
}
