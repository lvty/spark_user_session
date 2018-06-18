package com.spark.user_session.dao;

import com.spark.user_session.domain.SessionRandomExtract;

public interface ISessionRandomExtractDao {
    /**
     * session随机抽取模块聚合统计结果
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}
