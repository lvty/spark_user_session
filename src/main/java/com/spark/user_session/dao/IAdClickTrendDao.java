package com.spark.user_session.dao;

import com.spark.user_session.domain.AdClickTrend;

import java.util.List;

/**
 * 广告实时趋势接口
 */
public interface IAdClickTrendDao {
    void updateBatch(List<AdClickTrend> list);
}
