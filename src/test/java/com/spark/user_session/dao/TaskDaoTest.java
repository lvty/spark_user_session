package com.spark.user_session.dao;

import com.spark.user_session.dao.impl.DaoFactory;
import com.spark.user_session.domain.Task;

public class TaskDaoTest {
    public static void main(String[] args) {
        ITaskDao taskDao = DaoFactory.getTaskDao();
        Task byId = taskDao.findById(1);
        System.out.println(byId);
    }
}
