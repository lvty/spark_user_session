package com.spark.user_session.session_analysis;

import com.alibaba.fastjson.JSONObject;
import com.spark.user_session.conf.ConfigurationManager;
import com.spark.user_session.constant.Constants;
import com.spark.user_session.dao.*;
import com.spark.user_session.dao.impl.*;
import com.spark.user_session.domain.*;
import com.spark.user_session.util.*;
import com.sun.org.apache.bcel.internal.generic.ARRAYLENGTH;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session数据分析
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1.时间范围：起始日期-结束日期
 * 2.性别：男或者女
 * 3.年龄范围
 * 4.职业：多选
 * 5.城市：多选
 * 6.搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7.点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * spark作业如何接受用户创建的任务？
 * J2EE平台在接收用户创建的任务的请求之后，会将任务信息插入到mysql的task表中，任务参数以json的格式封装在task_param字段中
 * 接着J2EE平台执行spark-submit shell脚本，并将taskid作为参数传递给spark submit shell脚本，脚本可以接收参数，并传递
 * 进入main函数的args数组
 */
public class UserVisitSessionAnalysisSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{TwiceSortedKey.class})
                .setAppName(ConfigurationManager.getProperty(Constants.SPARK_CONF_SESSION_APPNAME));
        SparkUtils.setMaster(conf);

        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(jsc.sc());

        //生成模拟数据
        SparkUtils.mockData(jsc, sqlContext);

        //创建需要使用的Dao组件
        ITaskDao taskDao = DaoFactory.getTaskDao();

        /**
         * 如果要进行session粒度的数据聚合，首先要从user_visist_action表中，查询出来指定日期范围内的行为数据，如果要根据用户在创建
         * 任务时指定的参数，来进行数据过滤和筛选，那么就首先要查询出来指定的任务
         */
        Long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
        if(taskId == null){
            System.out.println(new Date() + ": can't find this taskId with [" + taskId + "]");
            return ;
        }
        Task task = taskDao.findById(taskId);
        //获取到用户创建的参数
        JSONObject taskparam = JSONObject.parseObject(task.getTask_param());

        //获取到执行日期范围内的行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskparam);
        JavaPairRDD<String, Row> sessionid2ActionRDD = SparkUtils.getSessionid2ActionRDD(actionRDD);
        /*System.out.println("actionRDD====" + actionRDD.count());*/

        /**
         * 首先，可以将行为数据，按照session_id进行groupByKey分组，此时的数据的粒度就是session粒度了，然后，可以将session粒度的数据
         * 与用户信息的数据join，就可以获取到全量数据信息
         */
        JavaPairRDD<String, String> aggFullInfos = aggregateByKeySession(sqlContext, actionRDD);
        //测试
        /*System.out.println(aggFullInfos.count());
        for(Tuple2<String,String> tuple:aggFullInfos.take(10)){
            System.out.println(tuple._2);
        }*/


        /**
         *接着就要针对session粒度的聚合数据按照使用者指定的筛选参数进行数据过滤
         *
         * 此时，进行重构设计，过滤的同时进行Accummulator的计算
         */

        //获取累加器
        Accumulator<String> accumulator = jsc.accumulator("", new SessionAggrStatAccumulator());
        JavaPairRDD<String, String> filteredAllFullInfos = filterSessionAndAccuUpdate(aggFullInfos, taskparam, accumulator);
        //测试
        System.out.println(filteredAllFullInfos.count());
        /*for(Tuple2<String,String> tuple:filteredAllFullInfos.take(10)){
            System.out.println(tuple._2);
        }*/

        JavaPairRDD<String, Row> session2detailRDD = getSession2DetailRDD(filteredAllFullInfos, sessionid2ActionRDD);

        //随机抽取算法的实现
        randomExtractSession(jsc,taskId, filteredAllFullInfos, sessionid2ActionRDD);

        /**
         * session聚合统计(统计出访问时长和访问步长，各个区间的session数量占总session数量的比例)
         *
         * 如果不进行重构，直接来实现，思路：
         *      actionRDD映射成<Sessionid,Row>的格式，计算每个sessionid对应的访问时长和访问步长，生成一个新的RDD
         *      遍历该RDD，更新自定义Accummulator中对应的值，并且计算各个区间的比例，将最后的结果，写入到mysql对应的表
         * 问题：
         *      1.actionRDD映射成<Sessionid,Row>的格式，前面已经实现过
         *      2.已经有session数据了，还有必要为了session的聚合单独再遍历一遍session？
         * 重构实现的思路：
         *      1.不要生成任何的RDD(每次都是处理上亿条数据)
         *      2.不要去单独遍历一遍session的数据(处理上千万的数据)
         *      3.可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         *      4.在进行过滤的时候，本来就要遍历所有的聚合session的信息，此时，就可以在某个session通过筛选条件后
         *          将其访问时长和访问步长，累加到自定义的Accummulator上面去
         * 两种思路，节省了大量时间
         * 准则：
         *      1.尽量少生成RDD
         *      2.尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面实现多个需要做的功能
         *      3.尽量少对RDD进行shuffle算子
         */

        /**
         * 注意：对于Accummulator这种分布式的累加计算的变量的使用，从Accummulator中获取数据插入数据库的时候，一定一定是在
         *  某一个action操作之后再进行，否则在action之前执行的话，累加器根本就没有value
         */
        //计算各个范围的占比，并写入mysql中
        calculateAndPersistMysqlAggrStat(taskId, accumulator.value());


        /**
         *获取top10热门品类
         */
        List<Tuple2<TwiceSortedKey, String>> top10Category = getTop10Category(taskId, session2detailRDD);

        /**
         * 获取top10活跃session
         */
        getTop10Session(jsc,taskId,session2detailRDD,top10Category);

        jsc.close();
    }

    /**
     * 获取top10活跃session
     * @param taskId
     * @param session2detailRDD
     */
    private static void getTop10Session(JavaSparkContext jsc,Long taskId,
        JavaPairRDD<String, Row> session2detailRDD,List<Tuple2<TwiceSortedKey, String>> top10Category) {

        /**
         * 第一步：将top10热门品类的id生成一份RDD
         */
        ArrayList<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for(Tuple2<TwiceSortedKey, String> t : top10Category){
            String countInfos = t._2;
            Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfos, "\\|", Constants.FIELD_CATEGORY_ID));

            Long clickcount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfos, "\\|", Constants.FIELD_CLICK_COUNT));
            top10CategoryIdList.add(new Tuple2<>(categoryid,categoryid));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD = jsc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步：计算top10 品类被各session点击的次数
         */
        JavaPairRDD<Long, String> categoryClickCnts = session2detailRDD.groupByKey().flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> t) throws Exception {
                        String sessionid = t._1;
                        Iterator<Row> it = t._2.iterator();

                        HashMap<Long, Long> categoryCountMap = new HashMap<>();

                        while (it.hasNext()) {
                            Row row = it.next();
                            if (row.get(6) != null) {
                                Long categoryId = (Long) row.get(6);
                                //计算出该session对每个品类的点击次数
                                Long cnt = categoryCountMap.getOrDefault(categoryId, 0L);
                                cnt++;
                                categoryCountMap.put(categoryId, cnt);
                            }
                        }

                        //返回结果 <categoryid,sessionid,count>
                        ArrayList<Tuple2<Long, String>> res = new ArrayList<>();
                        for (Map.Entry<Long, Long> en : categoryCountMap.entrySet()) {
                            Long categoryid = en.getKey();
                            Long cnt = en.getValue();
                            res.add(new Tuple2<>(categoryid, sessionid + "," + cnt));
                        }

                        return res;
                    }
                });
        //获取到top10热门品类，被各个session点击的次数 top10CategorySessionCountRDD
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = categoryClickCnts.join(top10CategoryIdRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Long>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Long>> t) throws Exception {

                        return new Tuple2<Long, String>(t._1, t._2._1);
                    }
                });

        /**
         * 第三步：分组取topN算法实现，获取每个品类的top10
         *
         */
        JavaPairRDD<Long, Iterable<String>> top10CategoryGroupedRDD = top10CategorySessionCountRDD.groupByKey();
        JavaPairRDD<String, String> top10sessionRDD = top10CategoryGroupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> t) throws Exception {
                Long categoryId = t._1;
                Iterator<String> it = t._2.iterator();
                String[] top10Session = new String[10];
                while (it.hasNext()) {
                    String[] split = it.next().split(",");
                    String sessionid = split[0];
                    Long cnts = Long.valueOf(split[1]);

                    //获取top 10
                    for (int i = 0; i < top10Session.length; i++) {

                        if (top10Session[i] == null) {
                            //此时该位置还没有数值
                            top10Session[i] = it.next();//把信息直接塞进去
                            break;
                        } else {
                            Long _cnt = Long.valueOf(top10Session[i].split(",")[1]);

                            if (cnts > _cnt) {
                                //那么后移
                                for (int j = top10Session.length - 1; j > i; j--) {
                                    top10Session[j] = top10Session[j - 1];
                                }
                                top10Session[i] = it.next();
                                break;
                            }
                        }
                    }
                }

                //将数据写入mysql表
                ArrayList<Tuple2<String, String>> list = new ArrayList<>();
                ITop10SessionDao top10SessionDaoImpl = Top10SessionDaoImpl.getTop10SessionDaoImpl();
                for (String sessioncnt : top10Session) {
                    String[] split = sessioncnt.split(",");
                    String sessionid = split[0];
                    Long cnt = Long.valueOf(split[1]);

                    Top10Session top10SessionInstance = new Top10Session();
                    top10SessionInstance.setCategory_id(categoryId);
                    top10SessionInstance.setSessionid(sessionid);
                    top10SessionInstance.setTask_id(taskId);
                    top10SessionInstance.setClick_count(cnt);

                    //插入mysql表
                    top10SessionDaoImpl.insert(top10SessionInstance);

                    //放入list中
                    list.add(new Tuple2<>(sessionid, sessionid));
                }
                return list;
            }
        });


        /**
         * 第四步：获取top10活跃session的明细数据，并写入mysql中
         */
        top10sessionRDD.join(session2detailRDD).foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
                Row row = t._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTask_id(taskId);
                sessionDetail.setUser_id(row.getLong(1));
                sessionDetail.setSession_id(row.getString(2));
                sessionDetail.setPage_id(row.getLong(3));
                sessionDetail.setAction_time(row.getString(4));
                sessionDetail.setSearch_keyword(row.getString(5));
                sessionDetail.setClick_category_id((Long) row.get(6) == null ? 0L : (Long) row.get(6));
                sessionDetail.setClick_product_id((Long) row.get(7) == null ? 0L : (Long) row.get(7));
                sessionDetail.setOrder_category_ids(row.getString(8));
                sessionDetail.setOrder_product_ids(row.getString(9));
                sessionDetail.setPay_category_ids(row.getString(10));
                sessionDetail.setPay_product_ids(row.getString(11));

                ISessionDeatilDao sessionDetailImpl =
                        SessionDetailImpl.getSessionDetailImpl();
                sessionDetailImpl.insert(sessionDetail);

            }
        });

    }

    private static List<Tuple2<TwiceSortedKey, String>> getTop10Category(Long taskId, JavaPairRDD<String, Row> session2detailRDD) {
        /**
         * 第一步：获取符合条件的session访问过的所有品类
         * //获取符合条件的session的访问明细
         * 已经重构了。。。
         */

        //获取session访问过的所有品类id,访问过指的是点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryIdRDD = session2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> t) throws Exception {
                Row session = t._2;

                //定义返回结果
                ArrayList<Tuple2<Long, Long>> list = new ArrayList<>();

                //获取数据
                Object o = session.get(6);
                if (o != null) {
                    Long clickCategoryId = (Long) o;
                    list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                }

                String orderCategoryIds = session.getString(8);
                if (orderCategoryIds != null) {
                    String[] split = orderCategoryIds.split(",");
                    for (String s : split) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(s), Long.valueOf(s)));
                    }
                }

                String payCategoryIds = session.getString(10);
                if (payCategoryIds != null) {
                    String[] split = payCategoryIds.split(",");
                    for (String s : split) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(s), Long.valueOf(s)));
                    }
                }

                return list;
            }
        });

        /**
         * 去重处理
         * 如果不去重的话，会出现重复的categoryid,排序会对重复的categoryid以及countinfo进行排序，最终会拿到重复的数据
         */
        categoryIdRDD = categoryIdRDD.distinct();


        /**
         * 计算各品类的点击 下单和支付次数，先对访问明细数据进行过滤，分别过滤出点击 下单 支付行为 然后通过算子来进行计算
         */
        //计算点击次数
        JavaPairRDD<Long, Long> clickedCategoryIdCounts = computeClickedCategoryIdCounts(session2detailRDD);
        //计算下单次数
        JavaPairRDD<Long, Long> orderedCategoryIdCounts = computeOrderedCategoryIdCounts(session2detailRDD);
        //计算支付次数
        JavaPairRDD<Long, Long> payedCategoryIdCounts = computePayedCategoryIdCounts(session2detailRDD);

        /**
         * 第三步，join各品类与它的点击 下单 和支付的次数
         *
         * 注意：categoryRDD中，包含了所有的符合条件的session，访问过的品类id 上面计算出来的三个次数，可能并不是包含所有品类的，比如说有的
         * 品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里就不能使用join操作，要使用leftjoin操作，就是说，如果categoryIdRDD不能join到自己的某一个数据，所以此时需要使用
         */

        JavaPairRDD<Long, String> category2CountRDD = joinCategoryAndData(categoryIdRDD, clickedCategoryIdCounts,
                orderedCategoryIdCounts, payedCategoryIdCounts);

        /**
         * 第四步：自定义二次排序的key
         */

        /**
         * 第五步：将数据映射成<SortKey,info>格式的RDD，然后进行二次排序
         */
        JavaPairRDD<TwiceSortedKey, String> twiceSortedKeyResRDD = category2CountRDD.mapToPair(new PairFunction<Tuple2<Long, String>, TwiceSortedKey, String>() {
            @Override
            public Tuple2<TwiceSortedKey, String> call(Tuple2<Long, String> t) throws Exception {
                String countInfos = t._2;
                Long clickcount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfos, "\\|", Constants.FIELD_CLICK_COUNT));
                Long ordercount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfos, "\\|", Constants.FIELD_ORDER_COUNT));
                Long paycount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfos, "\\|", Constants.FIELD_PAY_COUNT));
                TwiceSortedKey twiceSortedKey = new TwiceSortedKey();
                twiceSortedKey.setClickCount(clickcount);
                twiceSortedKey.setOrderCount(ordercount);
                twiceSortedKey.setPayCount(paycount);
                return new Tuple2<>(twiceSortedKey, countInfos);
            }
        }).sortByKey(false);

        /**
         * 第六步 取出top10 持久化到mysql数据库中
         */
        List<Tuple2<TwiceSortedKey, String>> top10CategoryLists = twiceSortedKeyResRDD.take(10);
        ITop10Category top10CategoryDaoImpl = Top10CategoryDaoImpl.getTop10CategoryDaoImpl();
        for (Tuple2<TwiceSortedKey, String> t : top10CategoryLists) {

            String countInfos = t._2;
            Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfos, "\\|", Constants.FIELD_CATEGORY_ID));

            Long clickcount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfos, "\\|", Constants.FIELD_CLICK_COUNT));
            Long ordercount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfos, "\\|", Constants.FIELD_ORDER_COUNT));
            Long paycount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfos, "\\|", Constants.FIELD_PAY_COUNT));
            TwiceSortedKey twiceSortedKey = new TwiceSortedKey();

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskid(taskId);
            top10Category.setCategoryid(categoryid);
            top10Category.setClickCount(clickcount);
            top10Category.setOrderCount(ordercount);
            top10Category.setPayCount(paycount);
            top10CategoryDaoImpl.insert(top10Category);
        }

        //返回，为下一步获取top10做准备
        return top10CategoryLists;
    }

    private static JavaPairRDD<String, Row> getSession2DetailRDD(JavaPairRDD<String, String> filteredAllFullInfos, JavaPairRDD<String, Row> sessionid2ActionRDD) {
        return filteredAllFullInfos.join(sessionid2ActionRDD).mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {

                        return new Tuple2<>(t._1, t._2._2);
                    }
                });
    }

    private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryIdRDD, JavaPairRDD<Long, Long> clickedCategoryIdCounts, JavaPairRDD<Long, Long> orderedCategoryIdCounts, JavaPairRDD<Long, Long> payedCategoryIdCounts) {
        return categoryIdRDD.leftOuterJoin(clickedCategoryIdCounts).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> t) throws Exception {
                        Long categoryId = t._1;
                        com.google.common.base.Optional<Long> clickCounts = t._2._2;
                        long v = 0L;
                        if (clickCounts.isPresent()) {
                            //有值
                            v = clickCounts.get();
                        }

                        String res = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + v;
                        return new Tuple2<>(categoryId, res);
                    }
                }).leftOuterJoin(orderedCategoryIdCounts)
                .mapToPair(
                        new PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>,
                                Long, String>() {
                            @Override
                            public Tuple2<Long, String> call(Tuple2<Long,
                                    Tuple2<String, com.google.common.base.Optional<Long>>> t) throws Exception {

                                Long categoryId = t._1;
                                String res = t._2._1;
                                com.google.common.base.Optional<Long> orderCounts = t._2._2;
                                long v = 0L;
                                if (orderCounts.isPresent()) {
                                    //有值
                                    v = orderCounts.get();
                                }

                                res += "|" + Constants.FIELD_ORDER_COUNT + "=" + v;
                                return new Tuple2<>(categoryId, res);
                            }
                        }).leftOuterJoin(payedCategoryIdCounts)
                .mapToPair(
                        new PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>,
                                Long, String>() {
                            @Override
                            public Tuple2<Long, String> call(
                                    Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>> t) throws Exception {

                                Long categoryId = t._1;
                                String res = t._2._1;
                                com.google.common.base.Optional<Long> payCounts = t._2._2;
                                long v = 0L;
                                if (payCounts.isPresent()) {
                                    //有值
                                    v = payCounts.get();
                                }

                                res += "|" + Constants.FIELD_PAY_COUNT + "=" + v;
                                return new Tuple2<>(categoryId, res);
                            }
                        });
    }

    private static JavaPairRDD<Long, Long> computePayedCategoryIdCounts(JavaPairRDD<String, Row> session2detailRDD) {
        JavaPairRDD<String, Row> payedActionRDD = session2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> t) throws Exception {
                return t._2.getString(10) != null ? true : false;
            }
        }).coalesce(20);

        JavaPairRDD<Long, Long> payedCategoryId = payedActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> t) throws Exception {
                        Row session = t._2;
                        String orderCategoryIds = session.getString(10);
                        String[] split = orderCategoryIds.split(",");
                        ArrayList<Tuple2<Long, Long>> list = new ArrayList<>();
                        for (String s : split) {
                            list.add(new Tuple2<>(Long.valueOf(s), 1L));
                        }
                        return list;
                    }
                });
        JavaPairRDD<Long, Long> payedCategoryIdCounts = payedCategoryId.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

        return payedCategoryIdCounts;
    }

    private static JavaPairRDD<Long, Long> computeOrderedCategoryIdCounts(JavaPairRDD<String, Row> session2detailRDD) {
        JavaPairRDD<String, Row> orderedActionRDD = session2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> t) throws Exception {
                return t._2.getString(8) != null ? true : false;
            }
        });

        JavaPairRDD<Long, Long> orderedCategoryId = orderedActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> t) throws Exception {
                        Row session = t._2;
                        String orderCategoryIds = session.getString(8);
                        String[] split = orderCategoryIds.split(",");
                        ArrayList<Tuple2<Long, Long>> list = new ArrayList<>();
                        for (String s : split) {
                            list.add(new Tuple2<>(Long.valueOf(s), 1L));
                        }
                        return list;
                    }
                });
        JavaPairRDD<Long, Long> orderedCategoryIdCounts = orderedCategoryId.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });
        return orderedCategoryIdCounts;
    }

    private static JavaPairRDD<Long, Long> computeClickedCategoryIdCounts(JavaPairRDD<String, Row> session2detailRDD) {
        JavaPairRDD<String, Row> clickedActionRDD = session2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> t) throws Exception {
                return t._2.get(6) != null ? true : false;
            }
        });

        JavaPairRDD<Long, Long> clickedCategoryId = clickedActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> t) throws Exception {
                return new Tuple2<>(t._2.getLong(6), 1L);
            }
        });
        JavaPairRDD<Long, Long> clickedCategoryIdCounts = clickedCategoryId.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

        return clickedCategoryIdCounts;
    }



    /**
     * 随机抽取session
     *
     * @param jsc
     * @param filteredAllFullInfos
     */
    private static void randomExtractSession(JavaSparkContext jsc, final long taskid,
                                             JavaPairRDD<String, String> filteredAllFullInfos,
                                             JavaPairRDD<String, Row> sessionid2ActionRDD) {
        //第一步，计算每天每小时的session数量
        //返回的是<yyyy-MM-dd_HH,sessionid>
        JavaPairRDD<String, String> allFullInfosWithStartTimeAsKey = filteredAllFullInfos.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {

                String aggInfo = t._2;
                String starttime = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(starttime);

                return new Tuple2<>(dateHour, aggInfo);
            }
        });

        /**
         * 计算出每天每小时的session数量，然后是每天每小时的session抽取索引，
         * 遍历每天每小时的session抽取出session的聚合数据，写入session_random_extract表
         */

        Map<String, Object> countMap = allFullInfosWithStartTimeAsKey.countByKey();

        //实现按时间比例随机抽取算法，计算每天每小时要抽取的session的索引
        //将<yyyy-MM-dd_HH,count> 转换为<yyyy-MM-dd,<HH,count>>的格式


        //调优点：map做成广播变量
        HashMap<String, Map<String, Long>> dayHourCountMap = new HashMap<>();


        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dayHour = countEntry.getKey();
            Long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            String[] split = dayHour.split("_");
            String day = split[0];
            String hour = split[1];

            Map<String, Long> hourcountmap = dayHourCountMap.get(day);
            if (hourcountmap == null) {
                hourcountmap = new HashMap<>();
                dayHourCountMap.put(day, hourcountmap);
            }
            hourcountmap.put(hour, count);

        }

        //广播变量
        final Broadcast<HashMap<String, Map<String, Long>>> dayHourCountMapBroadCastVariable = jsc.broadcast(dayHourCountMap);

        dayHourCountMap = dayHourCountMapBroadCastVariable.value();

        //System.out.println(dayHourCountMap.toString() + "dayHourCountMap");
        //算法实现
        //先按照天数平分
        long extractNumberPerDay = 100 / dayHourCountMap.size();


        //定义每一天抽取的索引,比如 <date,<hour,(3,5,20,102)>>
        final Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();
        for (Map.Entry<String, Map<String, Long>> m : dayHourCountMap.entrySet()) {
            Map<String, Long> hourcountmap = m.getValue();
            String date = m.getKey();

            //计算这一天的session总数
            long sessionCount = 0L;
            for (long hc : hourcountmap.values()) {
                sessionCount += hc;
            }
            //{2018-06-04={00=19, 11=33, 22=32, 12=37, 01=23, 13=36}
            Map<String, List<Integer>> hourExtractMapAndList = dateHourExtractMap.get(date);
            if (hourExtractMapAndList == null) {
                hourExtractMapAndList = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMapAndList);
            }

            //遍历每小时的session
            for (Map.Entry<String, Long> hct : hourcountmap.entrySet()) {
                long hourCnt = hct.getValue();
                String hour = hct.getKey();

                //计算每小时占比，直接乘以每天要抽取的数量，就可以计算粗，当前小时需要抽取的session数量
                int hourExtractNumber = (int) (((double) hourCnt / (double) sessionCount) * extractNumberPerDay);

                //限副
                if (hourExtractNumber > hourCnt) {
                    hourExtractNumber = (int) hourCnt;
                }


                List<Integer> extractIndexList = hourExtractMapAndList.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMapAndList.put(hour, extractIndexList);
                }

                //生成随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) hourCnt);

                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) hourCnt);
                    }
                    extractIndexList.add(extractIndex);
                }
            }

            //System.out.println("extractIndexList" + hourExtractMapAndList.toString());

        }

        //System.out.println(dateHourExtractMap.size()+ "==================");

        //遍历每天每小时的session，然后根据随机索引进行抽取
        JavaPairRDD<String, Iterable<String>> allFullInfosWithStartTimeAsKeyRDD = allFullInfosWithStartTimeAsKey.groupByKey();
        //System.out.println(allFullInfosWithStartTimeAsKeyRDD.count()+"+++++count ==========");
        /**
         * 使用flatmap算子，遍历所有的<datehour,(session aggrInfo)> 格式的数据，遍历每天每小时的session，如果发现某一个session恰巧
         * 在我们指定的这天这一个小时的随机抽取索引行，那么就抽取该session，直接写入mysql的random_extract_session表，将抽取出来的session_id
         * 返回回来，形成一个新的javaRDD<String>，最后一步，使用抽取出来的sessionid join visit明细表，写入session表
         */
        JavaPairRDD<String, String> sessionidPairedRDD = allFullInfosWithStartTimeAsKeyRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
                ArrayList<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
                try {
                    Iterator<String> it = t._2.iterator();
                    String[] split = t._1.split("_");
                    String date = split[0];
                    String hour = split[1];

                    List<Integer> list = dateHourExtractMap.get(date).get(hour);
                    //System.out.println("list random" + list.toString());
                    ISessionRandomExtractDao sessionRandomExtractDaoImpl = SessionRandomExtractDaoImpl.getSessionRandomExtractDaoImpl();
                    int idx = 0;
                    while (it.hasNext()) {
                        String sessionAggrInfo = it.next();
                        if (list.contains(idx)) {
                            String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                            //插入数据库
                            SessionRandomExtract sre = new SessionRandomExtract();
                            sre.setTask_id(taskid);
                            sre.setSession_id(sessionId);
                            sre.setStart_time(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                            sre.setSearch_keywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                            sre.setClick_category_ids(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CATEGORY_IDS));
                            sessionRandomExtractDaoImpl.insert(sre);

                            extractSessionIds.add(new Tuple2<>(sessionId, sessionId));
                        }
                        idx++;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                return extractSessionIds;
            }
        });

        //获取抽取出来的session明细数据
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = sessionidPairedRDD.join(sessionid2ActionRDD);

        /*extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
                Row row = t._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTask_id(taskid);
                sessionDetail.setUser_id(row.getLong(1));
                sessionDetail.setSession_id(row.getString(2));
                sessionDetail.setPage_id(row.getLong(3));
                sessionDetail.setAction_time(row.getString(4));
                sessionDetail.setSearch_keyword(row.getString(5));
                sessionDetail.setClick_category_id((Long) row.get(6) == null ? 0L : (Long) row.get(6));
                sessionDetail.setClick_product_id((Long) row.get(7) == null ? 0L : (Long) row.get(7));
                sessionDetail.setOrder_category_ids(row.getString(8));
                sessionDetail.setOrder_product_ids(row.getString(9));
                sessionDetail.setPay_category_ids(row.getString(10));
                sessionDetail.setPay_product_ids(row.getString(11));

                ISessionDeatilDao sessionDetailImpl =
                        SessionDetailImpl.getSessionDetailImpl();
                sessionDetailImpl.insert(sessionDetail);
            }
        });*/

        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> t) throws Exception {
                ArrayList<SessionDetail> sessionDetails = new ArrayList<>();
                while(t.hasNext()){
                    Row row = t.next()._2._2;
                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTask_id(taskid);
                    sessionDetail.setUser_id(row.getLong(1));
                    sessionDetail.setSession_id(row.getString(2));
                    sessionDetail.setPage_id(row.getLong(3));
                    sessionDetail.setAction_time(row.getString(4));
                    sessionDetail.setSearch_keyword(row.getString(5));
                    sessionDetail.setClick_category_id((Long) row.get(6) == null ? 0L : (Long) row.get(6));
                    sessionDetail.setClick_product_id((Long) row.get(7) == null ? 0L : (Long) row.get(7));
                    sessionDetail.setOrder_category_ids(row.getString(8));
                    sessionDetail.setOrder_product_ids(row.getString(9));
                    sessionDetail.setPay_category_ids(row.getString(10));
                    sessionDetail.setPay_product_ids(row.getString(11));
                    sessionDetails.add(sessionDetail);
                }

                ISessionDeatilDao sessionDetailImpl =
                        SessionDetailImpl.getSessionDetailImpl();
                sessionDetailImpl.insert(sessionDetails);
            }
        });


    }

    /**
     * 计算各session范围占比，并写入mysql
     *
     * @param value
     */
    private static void calculateAndPersistMysqlAggrStat(Long taskId, String value) {
        long sessioncount = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visitlength_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visitlength_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visitlength_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visitlength_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visitlength_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visitlength_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visitlength_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visitlength_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visitlength_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));


        long steplength_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long steplength_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long steplength_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long steplength_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long steplength_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long steplength_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));


        double visitlength_1s_3s_ratio = NumberUtils.formatDouble((double) visitlength_1s_3s / (double) sessioncount, 2);
        double visitlength_4s_6s_ratio = NumberUtils.formatDouble((double) visitlength_4s_6s / (double) sessioncount, 2);
        double visitlength_7s_9s_ratio = NumberUtils.formatDouble((double) visitlength_7s_9s / (double) sessioncount, 2);
        double visitlength_10s_30s_ratio = NumberUtils.formatDouble((double) visitlength_10s_30s / (double) sessioncount, 2);
        double visitlength_30s_60s_ratio = NumberUtils.formatDouble((double) visitlength_30s_60s / (double) sessioncount, 2);
        double visitlength_1m_3m_ratio = NumberUtils.formatDouble((double) visitlength_1m_3m / (double) sessioncount, 2);
        double visitlength_3m_10m_ratio = NumberUtils.formatDouble((double) visitlength_3m_10m / (double) sessioncount, 2);
        double visitlength_10m_30m_ratio = NumberUtils.formatDouble((double) visitlength_10m_30m / (double) sessioncount, 2);
        double visitlength_30m_ratio = NumberUtils.formatDouble((double) visitlength_30m / (double) sessioncount, 2);

        double steplength_1_3_ratio = NumberUtils.formatDouble((double) steplength_1_3 / (double) sessioncount, 2);
        double steplength_4_6_ratio = NumberUtils.formatDouble((double) steplength_4_6 / (double) sessioncount, 2);
        double steplength_7_9_ratio = NumberUtils.formatDouble((double) steplength_7_9 / (double) sessioncount, 2);
        double steplength_10_30_ratio = NumberUtils.formatDouble((double) steplength_10_30 / (double) sessioncount, 2);
        double steplength_30_60_ratio = NumberUtils.formatDouble((double) steplength_30_60 / (double) sessioncount, 2);
        double steplength_60_ratio = NumberUtils.formatDouble((double) steplength_60 / (double) sessioncount, 2);

        //封装为domain对象
        SessionAggr sessionAggr = new SessionAggr();
        sessionAggr.setTaskid(taskId);
        sessionAggr.setSessioncount(sessioncount);

        sessionAggr.setVisitlength_1s_3s_ratio(visitlength_1s_3s_ratio);
        sessionAggr.setVisitlength_4s_6s_ratio(visitlength_4s_6s_ratio);
        sessionAggr.setVisitlength_7s_9s_ratio(visitlength_7s_9s_ratio);
        sessionAggr.setVisitlength_10s_30s_ratio(visitlength_10s_30s_ratio);
        sessionAggr.setVisitlength_30s_60s_ratio(visitlength_30s_60s_ratio);
        sessionAggr.setVisitlength_1m_3m_ratio(visitlength_1m_3m_ratio);
        sessionAggr.setVisitlength_3m_10m_ratio(visitlength_3m_10m_ratio);
        sessionAggr.setVisitlength_10m_30m_ratio(visitlength_10m_30m_ratio);
        sessionAggr.setVisitlength_30m_ratio(visitlength_30m_ratio);

        sessionAggr.setSteplength_1_3_ratio(steplength_1_3_ratio);
        sessionAggr.setSteplength_4_6_ratio(steplength_4_6_ratio);
        sessionAggr.setSteplength_7_9_ratio(steplength_7_9_ratio);
        sessionAggr.setSteplength_10_30_ratio(steplength_10_30_ratio);
        sessionAggr.setSteplength_30_60_ratio(steplength_30_60_ratio);
        sessionAggr.setSteplength_60_ratio(steplength_60_ratio);


        //调用对应的dao，插入到mysql数据库中
        SessionAggrImpl.getSessionAggrStatDao().insert(sessionAggr);

    }


    /**
     * 过滤session数据
     *
     * @param aggFullInfos
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAccuUpdate(JavaPairRDD<String, String> aggFullInfos,
                                                                          final JSONObject taskparam,
                                                                          final Accumulator<String> accumulator) {

        //使用ValidUtils，首先将所有的筛选参数拼接成一个字符串
        String startAge = ParamUtils.getParam(taskparam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskparam, Constants.PARAM_END_AGE);
        String professional = ParamUtils.getParam(taskparam, Constants.PARAM_PROFESSIONAL);
        String city = ParamUtils.getParam(taskparam, Constants.PARAM_CITY);
        String sex = ParamUtils.getParam(taskparam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskparam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskparam, Constants.PARAM_CATEGORY_IDS);

        StringBuffer sb = new StringBuffer();//拼接格式
        sb.append(startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "");
        sb.append(endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "");
        sb.append(professional != null ? Constants.PARAM_PROFESSIONAL + "=" + professional + "|" : "");
        sb.append(city != null ? Constants.PARAM_CITY + "=" + city + "|" : "");
        sb.append(sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "");
        sb.append(keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "");
        sb.append(categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        String paramters = sb.toString();
        if (paramters.endsWith("|")) {
            paramters = paramters.substring(0, paramters.length() - 1);
        }

        final String _paramters = paramters;

        return aggFullInfos.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> t) throws Exception {
                //从tuple中获取聚合数据
                String aggInfo = t._2;

                //依次按照筛选条件进行过滤

                //_paramters   startage=10|endage=50
                //按照年龄范围进行过滤(startAge,endAge),没有设置就跳过
                if (!ValidUtils.between(aggInfo, Constants.FIELD_AGE, _paramters, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
                    return false;

                //按照职业范围进行过滤
                if (!ValidUtils.in(aggInfo, Constants.FIELD_PROFESSIONAL, _paramters, Constants.PARAM_PROFESSIONAL))
                    return false;

                //按照城市范围过滤
                if (!ValidUtils.in(aggInfo, Constants.FIELD_CITY, _paramters, Constants.PARAM_CITY)) return false;

                //按照性别过滤
                if (!ValidUtils.equal(aggInfo, Constants.FIELD_SEX, _paramters, Constants.PARAM_SEX)) return false;

                //按照搜索词过滤
                if (!ValidUtils.in(aggInfo, Constants.FIELD_SEARCH_KEYWORDS, _paramters, Constants.PARAM_KEYWORDS))
                    return false;

                //按照点击品类过滤
                if (!ValidUtils.in(aggInfo, Constants.FIELD_CATEGORY_IDS, _paramters, Constants.PARAM_CATEGORY_IDS))
                    return false;

                //经过了之前的过滤条件，说明是需要保留的session，根据这个session的访问时长和访问步长，进行统计，根据session对应的范围进行相应的累加计数
                accumulator.add(Constants.SESSION_COUNT);

                Long visitlength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggInfo, "\\|", Constants.FIELD_VISIT_LENGTH));

                Long steplength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                //计算
                calculateVisitLength(visitlength);
                calculatteStepLength(steplength);

                return true;
            }

            //计算访问时长范围
            private void calculateVisitLength(long visitlength) {
                if (visitlength >= 1 && visitlength <= 3) {
                    accumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitlength >= 4 && visitlength <= 6) {
                    accumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitlength >= 7 && visitlength <= 9) {
                    accumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitlength >= 10 && visitlength <= 30) {
                    accumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitlength > 30 && visitlength <= 60) {
                    accumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitlength > 60 && visitlength <= 3 * 60) {
                    accumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitlength > 3 * 60 && visitlength <= 10 * 60) {
                    accumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitlength > 10 * 60 && visitlength <= 30 * 60) {
                    accumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitlength > 30 * 60) {
                    accumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            //计算访问步长范围
            private void calculatteStepLength(long steplength) {
                if (steplength >= 1 && steplength <= 3) {
                    accumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (steplength >= 4 && steplength <= 6) {
                    accumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (steplength >= 7 && steplength <= 9) {
                    accumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (steplength >= 10 && steplength <= 30) {
                    accumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (steplength > 30 && steplength <= 60) {
                    accumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (steplength > 60) {
                    accumulator.add(Constants.STEP_PERIOD_60);
                }
            }

        });

    }


    /**
     * 对行为数据按照session粒度进行聚合
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateByKeySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        //输入为Row ,一个Row代表就是一行用户访问行为记录，比如一次点击或者搜索，需要将Row 映射为<sessionid,Row>格式
        JavaPairRDD<String, Row> mapPairedRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });

        //按照sessionid分组
        JavaPairRDD<String, Iterable<Row>> sessionidGroupedRDD = mapPairedRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类进行聚合
        JavaPairRDD<Long, String> partInfosOfSearchkwsAndClickCategorys = sessionidGroupedRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> t) throws Exception {
                String sessionId = t._1;
                Iterator<Row> it = t._2.iterator();
                StringBuilder searchKeywords = new StringBuilder();
                StringBuilder clickCategoryIds = new StringBuilder();
                Long userid = null;

                //session的起始和结束时间
                Date starttime = null;
                Date endtime = null;

                //session的访问步长
                long steplength = 0L;


                while (it.hasNext()) {
                    //提取每个访问行为的搜索词字段和点击品类字段
                    Row row = it.next();
                    if (row == null) {
                        continue;
                    }
                    String searchKeyword = row.getString(5);
                    Object clickCategoryId = row.get(6);

                    //Long clickCategoryId = row.getLong(6);     bug?????????????????======待解决

                    /**
                     * 说明，并不是每一行的访问行为都有searchkeyword和clickcategoryid两个字段的
                     *  只有搜索行为，是由searchkeyword字段的，而点击品类的行为才会有clickCategory字段的
                     *  所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
                     *
                     *  决定是否将搜索词或者点击品类拼接到字符串中去，首先要满足不能是null值，其次，之前的字符串中还没有搜索词或者点击品类id
                     */
                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywords.toString().contains(searchKeyword)) {
                            searchKeywords.append(searchKeyword + ",");
                        }
                    }

                    if (clickCategoryId != null) {
                        if (!clickCategoryIds.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIds.append(String.valueOf(clickCategoryId) + ",");
                        }
                    }

                    //计算session开始和结束的时间
                    Date actiontime = DateUtils.parseTime(row.getString(4));
                    if (starttime == null) {
                        starttime = actiontime;
                    }

                    if (endtime == null) {
                        endtime = actiontime;
                    }

                    if (actiontime.before(starttime)) {
                        starttime = actiontime;
                    }

                    if (actiontime.after(endtime)) {
                        endtime = actiontime;
                    }

                    //计算访问步长
                    steplength++;

                    //获取userID
                    if (userid == null) {
                        userid = row.getLong(1);
                    }

                }

                //计算session访问时长(s)
                long visitlength = (endtime.getTime() - starttime.getTime()) / 1000;


                //切除两遍的逗号
                String searchkd = StringUtils.trimComma(searchKeywords.toString());
                String clickCgIds = StringUtils.trimComma(clickCategoryIds.toString());

                //返回数据格式 <sessionid,partagginfo>,但是为了和userinfo进行聚合，所以此时就直接返回<userid,partagginfo>
                //返回数据格式：key=value|key = value
                String partAggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchkd + "|" +
                        Constants.FIELD_CATEGORY_IDS + "=" + clickCgIds + "|" +
                        Constants.FIELD_VISIT_LENGTH + "=" + visitlength + "|" +
                        Constants.FIELD_STEP_LENGTH + "=" + steplength + "|" +
                        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(starttime);
                return new Tuple2<>(userid, partAggInfo);
            }
        });

        //获取用户数据
        JavaRDD<Row> userInfoRDD = sqlContext.sql("select * from user_info").javaRDD();
        //映射操作
        JavaPairRDD<Long, Row> userInfoPairRdd = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });

        //session数据与用户数据join
        JavaPairRDD<Long, Tuple2<String, Row>> allInfos = partInfosOfSearchkwsAndClickCategorys.join(userInfoPairRdd);

        return allInfos.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> t) throws Exception {
                String partInfos = t._2._1;
                Row userRow = t._2._2;

                int age = userRow.getInt(3);
                String professional = userRow.getString(4);
                String city = userRow.getString(5);
                String sex = userRow.getString(6);

                String fullInfos = partInfos + "|" +
                        Constants.FIELD_AGE + "=" + age + "|" +
                        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                        Constants.FIELD_CITY + "=" + city + "|" +
                        Constants.FIELD_SEX + "=" + sex;

                String sessionId = StringUtils.getFieldFromConcatString(fullInfos, "\\|", Constants.FIELD_SESSION_ID);
                return new Tuple2<>(sessionId, fullInfos);
            }
        });
    }


}
