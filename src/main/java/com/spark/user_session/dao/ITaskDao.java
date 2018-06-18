package com.spark.user_session.dao;

import com.spark.user_session.domain.Task;

//任务管理接口DAO
public interface ITaskDao {

    /**
     * 根据主键查询任务
     * @param taskId
     * @return
     */
    Task findById(long taskId);

}
