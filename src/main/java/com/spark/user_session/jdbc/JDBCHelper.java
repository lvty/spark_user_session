package com.spark.user_session.jdbc;

import com.spark.user_session.conf.ConfigurationManager;
import com.spark.user_session.constant.Constants;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * jdbc辅助组件
 * 项目中，避免出现hard code 的字符 比如"com.mysql.jdbc.Driver"的字符，这些东西，需要通过常量来封装和使用
 */

public class JDBCHelper {
    /**
     * 第一步：在静态代码块中，直接加载数据库的驱动，加载驱动不要使用硬编码，因为不能保证项目是否会迁移到其他数据库中
     *        减少各个模块之间的耦合；
     *        使用常量接口中的某一个常量，来代表一个数值，然后当这个数值发生改变的时候，只要改变常量接口中的常量对应的
     *        值就可以了。
     *
     *        项目要尽量做成可以配置的，也就是说，该数据库的驱动，不是单一的放在常量接口中就OK了，最好的方式是方在外部
     *        的配置文件中，和代码彻底的分离，常量接口中，只是包含了这个值对应的key的名字。
     */
    static{
        try{
            Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 实现JDBCHelper的单例，内部封装了一个简单的数据库连接池，保证数据库连接池有且只要一份，所以就通过单例的方式
     *          保证只有一份数据库连接池
     */

    private static JDBCHelper instance = null;

    //模拟数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<>();

    //私有化构造方法,创建唯一的数据库连接池
    private JDBCHelper() {

        //第一步：通过读取配置文件的方式获取数据库连接池的大小
        int datasourcesize
                = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        Connection connection = null;
        //创建指定数量的数据库连接，并放入连接池中
        for (int i = 0; i < datasourcesize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                connection = DriverManager.getConnection(url, user, password);
                datasource.add(connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                if(connection != null){
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    //double-check
    public static JDBCHelper getInstance(){

        if(instance == null){
            synchronized (JDBCHelper.class){
                if(instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    //提供获取数据库连接的方法,如果在获取的时候，数据库连接池中的资源使用完了，那么需要实现一个等待机制，等待获取数据库连接
    public synchronized Connection getConnection(){
        while(datasource.size() == 0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    //开发CURD方法

    /**
     * 执行增删改SQL语句
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql,Object[] params){
        Connection connection = null;
        PreparedStatement psmt = null;
        int rtn = 0;
        try{
            connection = getConnection();
            psmt = connection.prepareStatement(sql);

            //设置参数
            for (int i = 0; i < params.length; i++) {
                psmt.setObject(i + 1,params[i]);

            }

            rtn =  psmt.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(connection != null){
                datasource.push(connection);
            }
        }
        return rtn;
    }

    //查询方法
    public void executeQuery(String sql,Object[] params,QueryCallBack callBack){
        Connection connection = null;
        PreparedStatement psmt = null;
        ResultSet rs = null;

        try{
            connection = getConnection();
            psmt = connection.prepareStatement(sql);

            //设置参数
            for (int i = 0; i < params.length; i++) {
                psmt.setObject(i + 1,params[i]);

            }

            rs = psmt.executeQuery();

            //处理结果
            callBack.process(rs);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(connection != null){
                datasource.push(connection);
            }
        }

    }

    /**
     * 内部类：查询回调的接口
     */
    public interface QueryCallBack{
        /**
         * 处理查询的结果
         * @param rs
         * @throws Exception
         */
        public void process(ResultSet rs) throws Exception;
    }

    /**
     * 批量执行SQL语句，短时间内要执行多条结构完全一模一样的SQL语句，只是参数不同，虽然可以使用preparedstatement这种方式，
     * 只编译一次，但是对于每次SQL都要向mysql发送一次网络请求
     *
     * 而批量执行SQL语句一次性可以发送多条SQL语句，执行的时候也只是编译一次就可以了
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的次数
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList){
        int[] rtn = null;
        Connection connection = null;
        PreparedStatement psmt = null;

        try{
            connection = getConnection();
            //1.使用connection对象，取消自动提交
            connection.setAutoCommit(false);
            psmt = connection.prepareStatement(sql);

            //2.设置参数
            for(Object[] o:paramsList){
                for (int i = 0; i < o.length; i++) {
                    psmt.setObject(i + 1,o[i]);
                }
                //添加批
                psmt.addBatch();
            }

            //3.执行批量
            rtn = psmt.executeBatch();

            //使用connection对象，批量提价SQL语句
            connection.commit();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(connection != null){
                datasource.push(connection);
            }
        }
        return rtn;
    }

}
