package com.spark.user_session.domain;

/**
 * 广告点击用户黑名单
 */
public class AdBlackList {

    private long userid;

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o instanceof AdBlackList == false){
            return false;
        }

        AdBlackList other = (AdBlackList) o;
        if(this.userid == other.userid) return true;

         return false;
    }

    @Override
    public int hashCode() {
        return (int)this.userid * 10;
    }
}
