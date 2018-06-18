package com.spark.user_session.session_analysis;

import com.spark.user_session.constant.Constants;
import com.spark.user_session.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * session  聚合统计Accummulator
 *  其实可以使用自定义的一些数据格式，比如说String，甚至可以是自定义的一些类，但前提是必须要序列化；
 *  然后就可以基于这种特殊的数据格式，实现自己复杂的分布式计算逻辑；各个task分布式运行，可以根据自己的需求，
 *  task给Accummulator传入不同的数值，根据不同的数值，去做复杂的逻辑。
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

    /**
     * 主要是用于数据初始化，我们这里就返回一个数值，就是初始化中，所有范围区间的数量都是0
     * 各个范围区间的统计数量的拼接，还是采用以往的key=value|key=value的连接串的格式
     * @param v
     * @return
     */
    @Override
    public String zero(String v) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * 以下两个方法主要就是实现更新操作
     * @param v1 初始化的串
     * @param v2 获取到的常量串，根据该串去更新原始串对应的数值
     * @return
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    /**
     * 统计逻辑的实现
     * @param v1 初始串也叫作连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {
        //校验：一为空的话，直接返回v2
        if(StringUtils.isEmpty(v1)){
            return v2;
        }

        //使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if(oldValue != null){
            //将范围区间内原有的数值累加1
            int newValue = Integer.valueOf(oldValue) + 1;

            //使用工具类，将v1中，v2对应的数值，设置成新的累加后的数值
            return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue));
        }

        return v1;

    }


}
