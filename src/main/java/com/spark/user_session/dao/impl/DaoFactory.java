package com.spark.user_session.dao.impl;

import com.spark.user_session.dao.ITaskDao;

//dao工厂类
public class DaoFactory {

    public static ITaskDao getTaskDao(){
        return new TaskDaoImpl();
    }
}
