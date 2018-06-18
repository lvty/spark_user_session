package com.spark.user_session.conf;


import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 1.对于简单的配置管理组件，只要开发一个类，可以在第一次访问它的时候，就从对应的properties文件中，读取配置项
 *      并提供外界获取某个配置key对应的value的方法
 * 2.如果是特别复杂的配置管理组件，那么可能需要使用一些软件设计中的设计模式，比如单例模式、解释器模式，
 *      可能需要管理多个不同的properties，甚至是xml类型的配置文件
 */
public class ConfigurationManager {

    private static Properties prop = new Properties();

    //配置管理组件，在静态代码块中只初始化一次，也就是只加载一次，效率高
    static{

        try{
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");

            //调用Properties的load方法，传入一个文件的inputstream输入流，即可将文件中的符合key = value 格式的配置项都加载到properties
            //加载之后，此时properties对象中就有了配置文件中所有的key-value对了，然后外界其实就可以通过properties对象获取指定key对应的value了
            prop.load(in);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key){
        try {
            return Integer.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        //转换失败，返回0
        return 0;
    }

    public static boolean getBoolean(String key){
        try {
            return Boolean.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        //转换失败，返回false
        return false;
    }

    public static long getLong(String key){
        try {
            return Long.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        //转换失败，返回false
        return 0;
    }

}
