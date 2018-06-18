package com.spark.user_session.domain;

/**
 * 各省top3热门广告
 */
public class AdProvinceTop3 {
    private String date;
    private String province;
    private long adid;
    private long clickcount;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public long getAdid() {
        return adid;
    }

    public void setAdid(long adid) {
        this.adid = adid;
    }

    public long getClickcount() {
        return clickcount;
    }

    public void setClickcount(long clickcount) {
        this.clickcount = clickcount;
    }
}
