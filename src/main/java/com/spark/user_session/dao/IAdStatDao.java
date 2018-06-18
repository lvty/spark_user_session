package com.spark.user_session.dao;

import com.spark.user_session.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计dao
 */
public interface IAdStatDao {
    void updateBatch(List<AdStat> list);
}
