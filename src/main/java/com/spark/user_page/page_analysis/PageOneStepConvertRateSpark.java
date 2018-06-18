package com.spark.user_page.page_analysis;

import com.alibaba.fastjson.JSONObject;
import com.spark.user_session.conf.ConfigurationManager;
import com.spark.user_session.constant.Constants;
import com.spark.user_session.dao.ITaskDao;
import com.spark.user_session.dao.impl.DaoFactory;
import com.spark.user_session.dao.impl.PageConvertDaoImpl;
import com.spark.user_session.domain.PageConvert;
import com.spark.user_session.domain.Task;
import com.spark.user_session.util.DateUtils;
import com.spark.user_session.util.NumberUtils;
import com.spark.user_session.util.ParamUtils;
import com.spark.user_session.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转换率模块spark作业
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        //1.构造Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(ConfigurationManager.getProperty(Constants.SPARK_CONF_PAGE_APPNAME));
        SparkUtils.setMaster(conf);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(jsc.sc());

        //2.生成模拟数据
        SparkUtils.mockData(jsc,sqlContext);

        //3.查询任务获取任务的参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
        if(taskId == null){
            System.out.println(new Date() + ": can't find this taskId with [" + taskId + "]");
            return ;
        }
        ITaskDao taskDao = DaoFactory.getTaskDao();
        Task task = taskDao.findById(taskId);
        //获取到用户创建的参数
        JSONObject taskparam = JSONObject.parseObject(task.getTask_param());

        //4.查询指定日期内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskparam);

        /**
         *5.对用户访问数据做映射，将其映射为<sessionid,访问行为>的格式
         *用户访问页面切片的生成，是要基于每一个session的访问数据来进行生成的，脱离了session，生成的页面访问切片是没有意义的
         * 举例：比如用户A访问了页面3和页面5
         * 用户B，访问了页面4和页面7
         * 使用者指定的页面筛选条件比如说是页面3->页面4->页面7;不能说是将页面3->页面4串起来的，作为一个页面切片来进行统计的
         * 所以说，页面切片的生成，肯定是要基于用户session粒度的
         */
        JavaPairRDD<String, Row> sessionid2ActionRDD = SparkUtils.getSessionid2ActionRDD(actionRDD);
        sessionid2ActionRDD = sessionid2ActionRDD.cache();
        //对<Sessionid,Row>RDD，做一次groupByKey操作，因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //6.最核心的一步，实现每个session的单跳页面切片的生成以及页面流的匹配算法
        JavaPairRDD<String, Long> pageSplitsRDD = pageSplitGenerateAndMatch(jsc, taskparam, sessionid2ActionsRDD);

        //7.获取每个切片的PV
        Map<String, Object> pageSplitPvMap = pageSplitsRDD.countByKey();

        //8.计算页面起始流的页面PV
        long startPV = getStartPV(taskparam, sessionid2ActionsRDD);

        //System.out.println(startPV);
        //9.计算页面切片的转化率
        Map<String, Double> convertRates = computePageSplitConvertRate(taskparam, startPV, pageSplitPvMap);

        //10.持久化
        persistConvertRate(taskId,convertRates);


        jsc.close();
    }

    /**
     * 实现页面单跳切片 以及页面流的匹配算法
     * @param jsc
     * @param taskparam
     * @param sessionid2ActionsRDD
     * @return
     */
    private static JavaPairRDD<String,Long> pageSplitGenerateAndMatch(
            JavaSparkContext jsc,JSONObject taskparam,
            JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD){

        String targetPageFlow = ParamUtils.getParam(taskparam, Constants.PARAM_TARGET_PAGE_FLOW);
        Broadcast<String> targetPageFlowBroadCast = jsc.broadcast(targetPageFlow);

        return sessionid2ActionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Long>() {
            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> t) throws Exception {

                //返回list
                ArrayList<Tuple2<String, Long>> res = new ArrayList<>();
                Iterator<Row> it = t._2.iterator();//用户访问行为

                //获取到使用者指定的页面流,指定的页面流比如说是1,2,3,4,5,6,7
                String[] targetPfs = targetPageFlowBroadCast.value().split(",");

                //需要计算出1->2 的转化率是多少，2->3的转化率是多少？。。。

                //1.由于拿到的session的访问行为默认是乱序的，需要我们按照【时间】顺序进行排序
                ArrayList<Row> tempForSortWithDate = new ArrayList<>();
                while(it.hasNext()){
                    tempForSortWithDate.add(it.next());
                }

                Collections.sort(tempForSortWithDate, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);
                        return (int)(DateUtils.parseTime(actionTime1).getTime()
                                - DateUtils.parseTime(actionTime2).getTime());
                    }
                });//排序完毕


                //2.页面切片的生成，以及页面流的匹配
                Long lastPageId = null;
                for(Row r:tempForSortWithDate){
                    Object o = r.get(3);
                    if(o != null){
                        Long pageid = (Long) o;

                        if(lastPageId == null){
                            lastPageId = pageid;
                            continue;
                        }

                        //生成一个页面切片  上一个切片_这一个切片
                        String pageSplit = lastPageId + "_" + pageid;

                        //对这个切片判断一下，是否在用户指定的页面流中
                        for(int i = 1;i < targetPfs.length;++i){

                            //比如 3,2,5,8,1    3_2  2_5  5_8  8_1
                            String targetPageSplit = targetPfs[i - 1] + "_" + targetPfs[i];
                            if(pageSplit.equals(targetPageSplit)){

                                //添加到返回结果
                                res.add(new Tuple2<>(pageSplit,1L));
                                break;
                            }
                        }

                        lastPageId = pageid;//更新上一个id

                    }
                }

                return res;
            }
        });
    }



    private static long getStartPV(JSONObject taskparam,
                                   JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD){

        String targetPageFlow = ParamUtils.getParam(taskparam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Long startpageid = Long.valueOf(targetPageFlow.split(",")[0]);

        return sessionid2ActionsRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {
            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> t) throws Exception {
                ArrayList<Long> res = new ArrayList<>();

                Iterator<Row> it = t._2.iterator();
                while(it.hasNext()){
                    Row row = it.next();
                    if(row.getLong(3) == startpageid){
                        res.add(startpageid);
                        //System.out.println(startpageid);
                    }
                }

                return res;
            }
        }).count();
    }


    /**
     * 计算页面切片的转化率
     * @param startPV
     * @param pageSplitPvMap
     * @return
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskparam,
                                                                   long startPV,
                                                                   Map<String, Object> pageSplitPvMap) {

        HashMap<String, Double> res = new HashMap<>();
        String[] targetpages = ParamUtils.getParam(taskparam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        long lastSplitPv = 0L;

        for (int i = 1; i < targetpages.length; i++) {

            String pageSplit = targetpages[i - 1] + "_" + targetpages[i];
            Long thisSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(pageSplit)));

            double convertRate = 0.0;
            if(i == 1){
                convertRate = NumberUtils.formatDouble( (double)thisSplitPv /(double)startPV ,2);
            }else{
                convertRate = NumberUtils.formatDouble( (double)thisSplitPv /(double)lastSplitPv ,2);
            }

            lastSplitPv = thisSplitPv;
            res.put(pageSplit,convertRate);
        }

        return res;
    }

    /**
     * 持久化转化率
     * @param convertRateMap
     */
    private static void persistConvertRate(long taskid, Map<String,Double> convertRateMap){

        StringBuffer sb = new StringBuffer();
        for(Map.Entry<String,Double> en:convertRateMap.entrySet()){
            String pageSplit = en.getKey();
            Double convertRate = en.getValue();
            sb.append(pageSplit + "=" + convertRate + "|");

        }
        String res = sb.toString();
        res = res.substring(0, res.length() - 1);

        PageConvert pageConvert = new PageConvert();
        pageConvert.setTaskid(taskid);
        pageConvert.setConvertRate(res);
        PageConvertDaoImpl.getPageConvertDaoImpl().insert(pageConvert);

    }

}
