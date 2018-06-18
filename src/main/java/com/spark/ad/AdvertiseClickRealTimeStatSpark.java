package com.spark.ad;

import com.google.common.base.Optional;
import com.spark.user_session.conf.ConfigurationManager;
import com.spark.user_session.constant.Constants;
import com.spark.user_session.dao.IAdUserClickCountDao;
import com.spark.user_session.dao.impl.*;
import com.spark.user_session.domain.*;
import com.spark.user_session.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计spark作业
 */
public class AdvertiseClickRealTimeStatSpark {
    public static void main(String[] args) {
        //1.构建javaStreamingContext对象，设置实时处理batch的interval
        SparkConf conf = new SparkConf().setAppName("").setMaster("local");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        //2.创建针对kafka数据来源的DStream
            //创建一份kafka参数的map
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LSIT,
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LSIT));

        //创建一个set，放入要读取的topic
        HashSet<String> topics = new HashSet<String>();
        String ts = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LSIT);
        String[] kafkaTopicsSplited = ts.split(",");
        for(String s:kafkaTopicsSplited){
            topics.add(s);
        }

        //实时日志 timestamp province city userid adid
        JavaPairInputDStream<String, String> inDStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams, topics);

        //2.5补充 基于动态黑名单对实时点击行为进行过滤（黑名单用户点击行为，进行过滤）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = getFilteredAdRealTimeLogDStream(inDStream);


        //3.计算出每隔5秒内的数据中，每天每个用户对每个广告的点击量,并高性能写入mysql中
        JavaPairDStream<String, Long> dailyUserAdClickCounts = getDailyUserAdClickCounts(filteredAdRealTimeLogDStream);

        //4.遍历每个batch的记录，查询mysql用户累计点击量，如果cnts > 100 则判定为黑名单用户，进行持久化；对原始数据进行过滤处理
        getBlackList(dailyUserAdClickCounts);

        //5.业务逻辑一、广告点击流量实时统计，计算每天各省各城市各广告的点击量，实时更新到mysql中的
        //设计出来的维度：日期  省份 城市 广告  J2EE系统可以很灵活的查看到指定搭配维度的实时数据
        //date province city userid adid
        //date_province_city_adid 作为key  1作为value 通过spark 直接统计出全局的点击次数，在spark集群和mysql中分别保留一份

        //获取到每天各省份各城市各广告的点击次数，每次计算出最新的值就在这个dstream，持久化到mysql中
        //结果(date_province_city_adid,clickcount)
        JavaPairDStream<String, Long> realTimeAdStatRes = computeRealAdStat(filteredAdRealTimeLogDStream);

        //6、业务功能二、计算每天每省份的top3热门广告，粒度降低
        calculateProvinceTop3Ad(realTimeAdStatRes);
        //7、业务功能三、实时统计每天每个广告在最近一小时的滑动窗口内的点击趋势（每分钟的点击量） 粒度更低
        //可以看到每个广告，最近一个小时内的每分钟的点击量，目的是看到每支广告的点击趋势
        calculateAdClickCountByWindow(inDStream);



        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }

    /**
     * 计算最近一个小时滑动窗口内的广告点击趋势
     * @param inDStream
     */
    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> inDStream) {

        inDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {

                //timestamp province city userid adid
                String[] split = t._2.split(" ");
                String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(split[0])));
                Long adid = Long.valueOf(split[4]);

                return new Tuple2<>(timeMinute + "_" + adid,1L);
            }
        })

        /**
         * 获取到每个batch rdd都会被映射成<yyyyMMddHHmm_adid,1L>的格式，每次出来一个新的batch，都要获取最近一个小时内的所有的batch
         * 然后根据key进行reduceBykey操作，统计出来最近一个小时内的每分钟各广告的点击次数，这就是一个小时内滑动窗口的广告点击趋势
         */
        .reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        },Durations.minutes(60),Durations.seconds(10))//每十秒钟更新最近一个小时的窗口内的数据
        .foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {

                        ArrayList<AdClickTrend> adClickTrends = new ArrayList<>();
                        while (iterator.hasNext()){
                            Tuple2<String, Long> t = iterator.next();
                            String[] split = t._1.split("_");
                            Long click_count = t._2;

                            String dateMinute = split[0];
                            Long adid = Long.valueOf(split[1]);

                            String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0,8)));

                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setAdid(adid);
                            adClickTrend.setMiute(minute);
                            adClickTrend.setDate(date);
                            adClickTrend.setClick_count(click_count);
                            adClickTrend.setHour(hour);

                            adClickTrends.add(adClickTrend);
                        }

                        AdClickTrendDaoImpl.getAdClickTrendDaoImpl().updateBatch(adClickTrends);
                    }
                });
            }
        });

    }

    /**
     * 计算每天各省份的top3热门广告
     * @param realTimeAdStatRes
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> realTimeAdStatRes) {
        //realTimeAdStatRes 每一个batch rdd 都代表了最新的全量的每天各省份各城市各广告的点击量
        JavaDStream<Row> rowsDStream = realTimeAdStatRes.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                //格式为dateKey + "_" + province + "_" + city + "_" + adid; clickcount
                //映射为dateKey + "_" + province + "_"  clickcount
                JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(
                        new PairFunction<Tuple2<String, Long>, String, Long>() {
                            @Override
                            public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
                                String[] split = t._1.split("_");
                                String date = split[0];
                                String province = split[1];
                                Long adid = Long.valueOf(split[3]);

                                return new Tuple2<>(date + "_" + province + "_" + adid, t._2);
                            }
                        });

                //重新降低粒度
                JavaPairRDD<String, Long> aggregatedRDD = mappedRDD.reduceByKey(
                        new Function2<Long, Long, Long>() {
                            @Override
                            public Long call(Long aLong, Long aLong2) throws Exception {
                                return aLong + aLong2;
                            }
                        });

                //注册临时表，使用spark sql的开窗函数 获取到各省份的top3热门广告
                JavaRDD<Row> rows = aggregatedRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> t) throws Exception {
                        String[] split = t._1.split("_");
                        String date = split[0];
                        //日期转换
                        String date1 = DateUtils.formatDate(DateUtils.parseDateKey(date));
                        String province = split[1];
                        Long adid = Long.valueOf(split[2]);
                        Long click_count = t._2;
                        return RowFactory.create(date1, province, adid, click_count);
                    }
                });

                StructType structType = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                        DataTypes.createStructField("click_count", DataTypes.LongType, true)
                ));

                HiveContext hiveContext = new HiveContext(rdd.context());
                DataFrame df = hiveContext.createDataFrame(rows, structType);
                //注册临时表
                df.registerTempTable("tmp_daily_ad_click_count_by_province");

                //配合开窗函数，统计各省份top3热门的广告
                String sql =
                        "select " +
                                "date," +
                                "province," +
                                "ad_id," +
                                "click_count" +
                                "from (" +
                                "select " +
                                "date," +
                                "province," +
                                "ad_id," +
                                "click_count," +
                                "row_number() over (partition by province order by click_count desc ) rank" +
                                "from tmp_daily_ad_click_count_by_province " +
                                ") t where t.rank <= 3";

                return hiveContext.sql(sql).javaRDD();

            }
        });

        //rowsDStream每次都是刷新出来各个省份最热门的top3广告，将其中的数据批量更新到mysql中
        rowsDStream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {
                rowJavaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> it) throws Exception {

                        ArrayList<AdProvinceTop3> list = new ArrayList<>();
                        while (it.hasNext()){
                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            Row row = it.next();
                            adProvinceTop3.setDate(row.getString(0));
                            adProvinceTop3.setProvince(row.getString(1));
                            adProvinceTop3.setAdid(Long.valueOf(String.valueOf(row.get(2))));
                            adProvinceTop3.setClickcount(Long.valueOf(String.valueOf(row.get(3))));

                            list.add(adProvinceTop3);

                        }

                        AdProvinceTop3DaoImpl.getAdProvinceTop3DaoImpl().updateBatch(list);
                    }
                });
            }
        });


    }


    /**
     * 计算广告实时统计
     * @param filteredAdRealTimeLogDStream
     */
    private static JavaPairDStream<String, Long> computeRealAdStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        JavaPairDStream<String, Long> aggregateDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String log = t._2;
                String[] logSplited = log.split(" ");

                //提取出日期(yyyyMMdd) userid adid
                Date date = new Date(Long.valueOf(logSplited[0]));
                String dateKey = DateUtils.formatDateKey(date);
                String province = logSplited[1];
                String city = logSplited[2];
                Long adid = Long.valueOf(logSplited[4]);

                //拼接key
                String key = dateKey + "_" + province + "_" + city + "_" + adid;
                return new Tuple2<>(key, 1L);
            }
        }).updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

            @Override
            public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                //比如 key 为 20180101_nn_ss_10001
                //首先根据optional判断，之前的这个key是否有相应 的状态，有进行累加
                long clickcount = 0L;
                if (optional.isPresent()) {
                    clickcount = optional.get();
                }
                //values是groupbykey对应的values
                for (Long l : values) {
                    clickcount += l;
                }
                return Optional.of(clickcount);
            }
        });
        aggregateDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> t) throws Exception {

                        ArrayList<AdStat> adStats = new ArrayList<>();
                        while(t.hasNext()){
                            Tuple2<String, Long> ads = t.next();
                            String[] split = ads._1.split("_");

                            String date = split[0];
                            String province = split[1];
                            String city = split[2];
                            Long adid = Long.valueOf(split[3]);

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAd_id(adid);
                            adStat.setClick_count(ads._2);

                            adStats.add(adStat);
                        }

                        AdStatDaoImpl.getAdStatDaoImpl().updateBatch(adStats);
                    }
                });
            }
        });

        return aggregateDStream;
    }

    /**
     * 基于动态黑名单对实时点击行为进行过滤
     * @param inDStream
     * @return
     */
    private static JavaPairDStream<String, String> getFilteredAdRealTimeLogDStream(JavaPairInputDStream<String, String> inDStream) {
        return inDStream.
                    transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                @Override
                public JavaPairRDD<String, String> call(JavaPairRDD<String, String> t) throws Exception {
                    //从mysql查询出所有黑名单用户
                    List<AdBlackList> adBlackLists = AdBlackListDaoImpl.getAdBlackListDaoImpl().findAll();
                    ArrayList<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
                    for(AdBlackList a:adBlackLists){
                        tuples.add(new Tuple2<>(a.getUserid(),true));
                    }

                    JavaSparkContext sc = new JavaSparkContext(t.context());
                    JavaPairRDD<Long, Boolean> blackListRDD = sc.parallelizePairs(tuples);

                    //将原始数据映射为<userid,str1|str2>格式
                    JavaPairRDD<Long, Tuple2<String, String>> sourceRDD = t.mapToPair(
                            new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
                            String log = t._2;
                            String[] logSplited = log.split(" ");
                            Long userid = Long.valueOf(logSplited[3]);
                            return new Tuple2<>(userid, t);
                        }
                    });

                    //将原始数据与黑名单数据进行左外链接
                    JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedSourceWithBlackList =
                            sourceRDD.leftOuterJoin(blackListRDD);
                    JavaPairRDD<String, String> res = joinedSourceWithBlackList.filter(
                            new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                @Override
                                public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> t) throws Exception {
                                    Optional<Boolean> options = t._2._2;
                                    if (options.isPresent() && options.get()) {
                                        return false;//是黑名单用户
                                    }
                                    return true;
                                }
                            }).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>,
                            Optional<Boolean>>>, String, String>() {
                        @Override
                        public Tuple2<String, String> call(Tuple2<Long,
                                Tuple2<Tuple2<String, String>, Optional<Boolean>>> t) throws Exception {
                            return t._2._1;
                        }
                    });

                    return res;
                }
            });
    }

    /**
     * 持久化黑名单记录
     * @param dailyUserAdClickCounts
     */
    private static void getBlackList(JavaPairDStream<String, Long> dailyUserAdClickCounts) {
        JavaPairDStream<String, Long> blackListDStream = dailyUserAdClickCounts.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> t) throws Exception {

                String[] split = t._1.split("_");
                //由yyyyMMdd 转换为yyyy-MM-dd格式
                String date = DateUtils.formatDate(DateUtils.parseDateKey(split[0]));
                Long userid = Long.valueOf(split[1]);
                Long adid = Long.valueOf(split[2]);

                //查询点击次数
                int clickCounts = AdUserClickCountDaoImpl.getAdUserClickCountDao()
                        .findClickCountByMultiKeys(date, userid, adid);
                //大于 100 返回true
                if(clickCounts >= 100){
                    return true;
                }
                return false;

            }
        });

        blackListDStream.map(new Function<Tuple2<String,Long>, Long>() {
            @Override
            public Long call(Tuple2<String, Long> t) throws Exception {
                String[] split = t._1.split("_");
                Long userid = Long.valueOf(split[1]);
                return userid;
            }
        }).transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {

            @Override
            public JavaRDD<Long> call(JavaRDD<Long> t) throws Exception {
                return t.distinct();
            }
        })
        //遍历黑名单，将黑名单动态增加到mysql中,前提需要去重
        .foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
            public void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> it) throws Exception {

                        ArrayList<AdBlackList> adBlackLists = new ArrayList<>();
                        while(it.hasNext()){
                            AdBlackList adBlackList = new AdBlackList();
                            adBlackList.setUserid(it.next());
                            adBlackLists.add(adBlackList);
                        }
                        AdBlackListDaoImpl.getAdBlackListDaoImpl().insertBatch(adBlackLists);

                    }
                });
            }
        });
    }


    /**
     * 计算出每隔5秒内的数据中，每天每个用户对每个广告的点击量
     * @param inDStream
     */
    private static JavaPairDStream<String, Long> getDailyUserAdClickCounts(JavaPairDStream<String, String> inDStream) {
        //对原始日志进行处理，返回<yyyyMMdd userid adid，1>的格式
        JavaPairDStream<String, Long> maped1RealLog = inDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String log = t._2;
                String[] logSplited = log.split(" ");

                //提取出日期(yyyyMMdd) userid adid
                Date date = new Date(Long.valueOf(logSplited[0]));
                String dateKey = DateUtils.formatDateKey(date);

                Long userid = Long.valueOf(logSplited[3]);
                Long adid = Long.valueOf(logSplited[4]);

                //拼接key
                String key = dateKey + "_" + userid + "_" + adid;
                return new Tuple2<String, Long>(key, 1L);
            }
        });

        //执行reduceByKey,获取到每隔5秒的batch中，当天每个用户对每支广告的点击次数
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = maped1RealLog.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> t) throws Exception {
                        ArrayList<AdUserClickCount> adUserClickCounts = new ArrayList<>();

                        while(t.hasNext()){
                            Tuple2<String, Long> aduser = t.next();
                            AdUserClickCount adUserClickCount = new AdUserClickCount();

                            Long cnts = aduser._2;
                            String[] split = aduser._1.split("_");
                            //由yyyyMMdd 转换为yyyy-MM-dd格式
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(split[0]));

                            Long userid = Long.valueOf(split[1]);
                            Long adid = Long.valueOf(split[2]);

                            adUserClickCount.setDate(date);
                            adUserClickCount.setUserid(userid);
                            adUserClickCount.setAdid(adid);
                            adUserClickCount.setClickcnts(cnts);

                            adUserClickCounts.add(adUserClickCount);
                        }

                        IAdUserClickCountDao dao = AdUserClickCountDaoImpl.getAdUserClickCountDao();
                        dao.updateBatch(adUserClickCounts);
                    }

                });

                return null;

            }
        });

        return dailyUserAdClickCountDStream;
    }
}
