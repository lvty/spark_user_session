package com.spark.user_session.dao;

import com.spark.user_session.domain.SessionDetail;

import java.util.List;

public interface ISessionDeatilDao {
    void insert(SessionDetail sessionDetail);

    void insert(List<SessionDetail> sessionDetails);
}
