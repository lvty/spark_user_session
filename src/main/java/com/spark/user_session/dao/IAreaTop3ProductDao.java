package com.spark.user_session.dao;

import com.spark.user_session.domain.AreaTop3Product;

import java.util.List;

public interface IAreaTop3ProductDao {
    void insertBatch(List<AreaTop3Product> list);
}
