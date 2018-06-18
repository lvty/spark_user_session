package com.spark.user_session.dao;


import com.spark.user_session.domain.AdProvinceTop3;

import java.util.List;

/**
 * 各省份top3热门广告接口Dao
 */
public interface IAdProvinceTop3Dao {
    /**
     * 如果存在这条记录，则删除这三条记录；插入当前数据
     * @param list
     */
    void updateBatch(List<AdProvinceTop3> list);
}
