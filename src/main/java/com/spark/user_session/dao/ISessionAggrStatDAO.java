package com.spark.user_session.dao;

import com.spark.user_session.domain.SessionAggr;

//session 聚合模块DAO
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     * @param sessionAggr
     */
    void insert(SessionAggr sessionAggr);
}
