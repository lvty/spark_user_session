package com.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.spark.user_session.conf.ConfigurationManager;
import com.spark.user_session.constant.Constants;
import com.spark.user_session.dao.IAreaTop3ProductDao;
import com.spark.user_session.dao.ITaskDao;
import com.spark.user_session.dao.impl.AreaTop3ProductDaoImpl;
import com.spark.user_session.dao.impl.DaoFactory;
import com.spark.user_session.domain.AreaTop3Product;
import com.spark.user_session.domain.Task;
import com.spark.user_session.util.ParamUtils;
import com.spark.user_session.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        //1.构造Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(ConfigurationManager.getProperty(Constants.SPARK_CONF_Product_APPNAME));
        SparkUtils.setMaster(conf);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(jsc.sc());


        //注册udf
        sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);
        sqlContext.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix", new RemoveRandomPrefix(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());


        //2.生成模拟数据
        SparkUtils.mockData(jsc, sqlContext);

        //3.查询任务获取任务的参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_Product);
        if (taskId == null) {
            System.out.println(new Date() + ": can't find this taskId with [" + taskId + "]");
            return;
        }
        ITaskDao taskDao = DaoFactory.getTaskDao();
        Task task = taskDao.findById(taskId);
        //获取到用户创建的参数
        JSONObject taskparam = JSONObject.parseObject(task.getTask_param());

        //4.获取到用户点击点击行为的RDD,在哪个尝试发生的点击行为
        JavaPairRDD<Long, Row> clickActionRDD = SparkUtils.getClickActionRDDByDate(sqlContext, taskparam);

        //5.从mysql中查询城市信息
        JavaPairRDD<Long, Row> cityInfoRDD = getCityInfoRDD(sqlContext);

        //6.join操作
        generateTmpclickAndCityInfos(sqlContext, clickActionRDD, cityInfoRDD);

        //7.查询各区域各商品的点击次数并拼接城市列表
        generateTempAreaProductClickCountTable(sqlContext);

        //8.生成各区域个商品的点击次数包含商品的完整信息
        generateTempAreaFullInfoWithProductClickCountTable(sqlContext);

        //9.获取各区域top3热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        //10.写入mysql数据库，数据量很少，可以collect到本地直接批量插入到mysql中
        List<Row> res = areaTop3ProductRDD.collect();
        persistAreaTop3Product(taskId,res);
        jsc.close();
    }

    private static void persistAreaTop3Product(long taskid,List<Row> res){

        IAreaTop3ProductDao areaTop3ProductDaoImpl = AreaTop3ProductDaoImpl.getAreaTop3ProductDaoImpl();
        ArrayList<AreaTop3Product> areaTop3Products = new ArrayList<>();
        for(Row r :res){
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(r.getString(0));
            areaTop3Product.setAreaLevel(r.getString(1));
            areaTop3Product.setProductid(r.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(r.get(3))));
            areaTop3Product.setCityInfos(r.getString(4));
            areaTop3Product.setProductName(r.getString(5));
            areaTop3Product.setProductStatus(r.getString(6));

            areaTop3Products.add(areaTop3Product);
        }

        areaTop3ProductDaoImpl.insertBatch(areaTop3Products);

    }


    /**
     * 获取各区域top3热门商品
     * @param sqlContext
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext){
        //A、华北、华东
        //B、华南、华中
        //C、西北、西南
        //D、东北

        //case when then else end
        String sql ="select " +
                        "area," +
                            "case " +
                                "when area = '华北' or area = '华东' then 'A级' " +
                                "when area = '华中' or area = '华南' then 'B级' " +
                                "when area = '西北' or area = '西南' then 'C级' " +
                                "else 'D级' " +
                            "end area_level," +
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status " +
                     "from " +
                     "(" +
                         "select " +
                             "area," +
                             "product_id," +
                             "click_count," +
                             "city_infos," +
                             "product_name," +
                             "product_status " +
                             "row_number() over (partition by area order by click_count) rank " +
                         "from temp_area_procuct_allinfos " +
                     ") temp where temp.rank <= 3";

        //本地模式开窗函数没法运行



        //执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);
        return df.javaRDD();
    }


    /**
     * 生成各区域个商品的点击次数包含商品的完整信息
     * @param sqlContext
     */
    private static void generateTempAreaFullInfoWithProductClickCountTable(SQLContext sqlContext) {
        String sql =   "select " +
                            "tapcc.area," +
                            "tapcc.product_id," +
                            "tapcc.click_count," +
                            "tapcc.city_infos," +
                            "pi.product_name," +
                            "if(get_json_object(pi.extend_info,'product_status') = '0','自营商品','第三方商品') product_status " +
                        "from temp_area_procuct_click_counts tapcc " +
                        "join product_info pi " +
                        "on tapcc.product_id = pi.product_id ";

        /*
        第七种方案
        JavaRDD<Row> flattedRDD = sqlContext.sql("select * from product_info").javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                ArrayList<Row> list = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    long productid = row.getLong(0);
                    String prefix_productid = i + "_" + productid;
                    Row _row = RowFactory.create(prefix_productid, row.get(1), row.get(2));
                    list.add(_row);
                }
                return list;
            }
        });

        //创建一张临时表
        StructType structType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.StringType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("product_status", DataTypes.StringType, true)
        ));

        sqlContext.createDataFrame(flattedRDD,structType).registerTempTable("temp_product_info");


        String sql1 =   "select " +
                            "tapcc.area," +
                            "remove_random_prefix(tapcc.product_id) product_id," +
                            "tapcc.click_count," +
                            "tapcc.city_infos," +
                            "pi.product_name," +
                            "if(get_json_object(pi.extend_info,'product_status') = 0,'自营商品','第三方商品') product_status " +
                        "from ( " +
                            "select " +
                                "area," +
                                "random(product_id,10) random_prefix" +
                                "click_count," +
                                "city_infos" +
                            "from temp_area_procuct_click_counts" +
                        ") tapcc " +
                        "join product_info pi " +
                        "on tapcc.product_id = pi.product_id ";*/




        //执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);
        df.registerTempTable("temp_area_procuct_allinfos");
    }


    /**
     * @param sqlContext
     */
    private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {

        String sql = "select area,product_id,count(1) click_count, " +
                "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
                "from tmp_click_product_basic_infos group by area,product_id";

        /*
        第四种方案
        String sql1 =
            "select " +
                "product_id_area,"+
                "count(click_count) click_cnt,"+
                "group_concat_distinct(city_infos) city_infos " +
            "from ( " +
                    "select " +
                        "remove_random_prefix(product_id_area) product_id_area," +
                        "click_count," +
                        "city_infos," +
                    "from ( " +
                        "select " +
                            "product_id_area " +
                            "count(1) click_count," +
                            "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos" +
                        "from ( " +
                            "select " +
                                "random_prefix(concat_long_string(product_id,area,':'),10) product_id_area," +
                                "city_id," +
                                "city_name," +
                            "from tmp_click_product_basic_infos " +
                        ") t1" +
                        "group by product_id_area " +
                    ") t2 "+
            ") t3 " +
            "group by product_id_area";*/



        //执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);
        df.registerTempTable("temp_area_procuct_click_counts");
    }

    /**
     * 关联点击行为数据与城市信息数据
     *
     * @param clickActionRDD
     * @param cityInfoRDD
     */
    private static void generateTmpclickAndCityInfos(SQLContext sqlContext, JavaPairRDD<Long, Row> clickActionRDD,
                                                     JavaPairRDD<Long, Row> cityInfoRDD) {
        //关联操作
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDDInfos = clickActionRDD.join(cityInfoRDD);
        //转换为JavaRDD<row>形式，才能转换为DataFrame
        JavaRDD<Row> row = joinedRDDInfos.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> t) throws Exception {
                Row clickAction = t._2._1;
                Row cityInfo = t._2._2;
                long product_id = clickAction.getLong(1);
                long city_id = cityInfo.getLong(0);
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(city_id, cityName, area, product_id);
            }
        });

        StructType structType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("city_id", DataTypes.LongType, true),
                DataTypes.createStructField("city_name", DataTypes.StringType, true),
                DataTypes.createStructField("area", DataTypes.StringType, true),
                DataTypes.createStructField("product_id", DataTypes.LongType, true)
        ));

        //转换为DataFrame
        DataFrame df = sqlContext.createDataFrame(row, structType);

        //注册为临时表
        df.registerTempTable("tmp_click_product_basic_infos");

        //1.bj
        //2.shanghai
        //1.去重
        //每个区域下每个商品的点击次数  返回的是1：bj,2：shanghai
        //两个函数
        //udf:concat2(),将两个字段拼接起来，使用指定的分隔符
        //UDAF；group_concat_distinct() 将一个分组内的多个字段值，使用逗号拼接起来，同时进行去重

    }

    /**
     * 使用spark SQL从mysql中查询城市信息
     *
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityInfoRDD(SQLContext sqlContext) {
        //构建mysql连接配置信息（直接从配置文件中获取）
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

        HashMap<String, String> prop = new HashMap<>();
        prop.put("url", url);
        prop.put("dbtable", "city_info");
        prop.put("user",user);
        prop.put("password",password);

        //通过sqlcontext从mysql中查询数据
        DataFrame cityInfo = sqlContext.read().format("jdbc").options(prop).load();

        return cityInfo.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
    }
}
