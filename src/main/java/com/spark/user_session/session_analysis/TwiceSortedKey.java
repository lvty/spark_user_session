package com.spark.user_session.session_analysis;

import scala.Serializable;
import scala.math.Ordered;

//自定义二次排序的key
public class TwiceSortedKey implements Ordered<TwiceSortedKey>, Serializable{

    private long clickCount;
    private long orderCount;
    private long payCount;


    @Override
    public int compare(TwiceSortedKey that) {
        if(this.clickCount - that.clickCount != 0){
            return (int)(this.clickCount - that.clickCount);
        }else if(this.orderCount - that.orderCount != 0){
            return (int)(this.orderCount - that.orderCount);
        }else if(this.payCount - that.payCount != 0){
            return (int)(this.payCount - that.payCount);
        }
        return 0;
    }

    @Override
    public int compareTo(TwiceSortedKey that) {
         return this.compare(that);
    }

    @Override
    public boolean $less(TwiceSortedKey that) {
        if(this.clickCount < that.clickCount){
            return true;
        }else if(this.clickCount == that.clickCount && this.orderCount < that.orderCount){
            return true;
        }else if(this.clickCount == that.clickCount
                && this.orderCount == that.orderCount
                && this.payCount < that.payCount){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $greater(TwiceSortedKey that) {
        if(this.clickCount > that.clickCount){
            return true;
        }else if(this.clickCount == that.clickCount && this.orderCount > that.orderCount){
            return true;
        }else if(this.clickCount == that.clickCount
                && this.orderCount == that.orderCount
                && this.payCount > that.payCount){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $less$eq(TwiceSortedKey that) {
        if($less(that)) return true;
        else if(this.clickCount == that.clickCount
                && this.orderCount == that.orderCount
                && this.payCount == that.payCount) return true;
        else return false;
    }

    @Override
    public boolean $greater$eq(TwiceSortedKey that) {
        if($greater(that)) return true;
        else if(this.clickCount == that.clickCount
                && this.orderCount == that.orderCount
                && this.payCount == that.payCount) return true;
        else return false;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
