package com.spark.user_session.util;

import com.alibaba.fastjson.JSONObject;
import com.spark.user_session.conf.ConfigurationManager;
import com.spark.user_session.constant.Constants;
import com.spark.user_session.session_analysis.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

public class SparkUtils {

    /**
     * 设置master
     * @param conf
     */
    public static void setMaster(SparkConf conf){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            conf.setMaster("local");
        }
    }

    /**
     * 生成模拟数据(只有本地模式，才会去生成模拟数据)
     * @param jsc
     * @param sc
     */
    public static void mockData(JavaSparkContext jsc, SQLContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(jsc, sc);
        }

    }

    /**
     * 获取sqlcontext，本地测试，生成的是sqlcontext对象，生产环境，生成hivecontext对象
     * @param sc
     * @return
     */
    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local == true) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 获取指定日期范围内的点击行为数据
     * @param sqlContext
     * @param taskparam
     * @return
     */
    public static JavaPairRDD<Long,Row> getClickActionRDDByDate(SQLContext sqlContext, JSONObject taskparam){
        String startDate = ParamUtils.getParam(taskparam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskparam, Constants.PARAM_END_DATE);

        String sql = "select city_id,click_product_id from user_visit_action where " +
                "click_product_id is not null " +
                "and date >= '" + startDate + "' and date <= '" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0),row);
            }
        });
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext
     * @param taskparam
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskparam) {

        String startDate = ParamUtils.getParam(taskparam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskparam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where date >= '" +
                startDate + "' and date <= '" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }

    /**
     * 获取<sessionid,用户访问行为>格式的数据
     * @param actionRDD 用户访问行为的RDD
     * @return <sessionid,用户访问行为>
     */
    public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {

                return new Tuple2<>(row.getString(2), row);
            }
        });
    }
}
