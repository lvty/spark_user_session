package com.spark.user_session.domain;

/**
 * 广告点击趋势
 */
public class AdClickTrend {
    private String hour;

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    private String date;
    private String miute;
    private long adid;
    private long click_count;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getMiute() {
        return miute;
    }

    public void setMiute(String miute) {
        this.miute = miute;
    }

    public long getAdid() {
        return adid;
    }

    public void setAdid(long adid) {
        this.adid = adid;
    }

    public long getClick_count() {
        return click_count;
    }

    public void setClick_count(long click_count) {
        this.click_count = click_count;
    }
}
