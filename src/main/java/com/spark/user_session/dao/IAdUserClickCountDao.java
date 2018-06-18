package com.spark.user_session.dao;

import com.spark.user_session.domain.AdUserClickCount;

import java.util.List;

/**
 * 用户广告点击量DAO
 */
public interface IAdUserClickCountDao {
    //批量更新用户点击量
    void updateBatch(List<AdUserClickCount> adUserClickCount);
    //通过多个关键词查询结果
    int findClickCountByMultiKeys(String date,long userid,long adid);
}
