package com.spark.user_session.jdbc;

/**
 * 单例模式的应用场景有哪些？
 * 1.配置管理组件，可以在读取大量的配置信息之后，使用单例模式的方式，就将配置信息仅仅保存在一个实例的实例变量中，
 *      这样就可以避免对于静态不变的配置信息，反复多次的读取
 * 2.jdbc辅助组件，全局就只有一个实例，实例中持有了一个内部的简单数据源，使用单例模式之后，就可以保证只有一个实例，
 *      那么数据源也就只有一个，不会重复创建多次数据源(数据库连接池)
 */
public class Singleton {
    private static Singleton instance = null;
    private Singleton(){}

    public static Singleton getInstance(){

        if(instance == null){
            synchronized (Singleton.class){
                if(instance == null){
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
