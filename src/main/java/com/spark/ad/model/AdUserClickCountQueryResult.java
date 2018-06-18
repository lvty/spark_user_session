package com.spark.ad.model;

public class AdUserClickCountQueryResult {
    private int cnt;//存储查询结果的条数
    private int clickcnt;//查询结果的点击次数

    public int getClickcnt() {
        return clickcnt;
    }

    public void setClickcnt(int clickcnt) {
        this.clickcnt = clickcnt;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
