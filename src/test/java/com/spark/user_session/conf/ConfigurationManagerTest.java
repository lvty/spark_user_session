package com.spark.user_session.conf;

import com.spark.user_session.conf.ConfigurationManager;

//配置管理组件测试类
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        System.out.println(ConfigurationManager.getProperty("k1"));
        System.out.println(ConfigurationManager.getProperty("k2"));
    }
}
