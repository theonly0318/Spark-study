package top.theonly.spark.jav.core.cases.secondsort;

import java.io.Serializable;

/**
 * 自定义排序的key
 * @author theonly
 */
public class MySort implements Comparable<MySort>, Serializable {

    private Integer firstNum;
    private Integer secondNum;

    public MySort() {
    }

    public MySort(Integer firstNum, Integer secondNum) {
        this.firstNum = firstNum;
        this.secondNum = secondNum;
    }

    public Integer getFirstNum() {
        return firstNum;
    }

    public void setFirstNum(Integer firstNum) {
        this.firstNum = firstNum;
    }

    public Integer getSecondNum() {
        return secondNum;
    }

    public void setSecondNum(Integer secondNum) {
        this.secondNum = secondNum;
    }

    @Override
    public int compareTo(MySort that) {
        if (this.firstNum.equals(that.firstNum)) {
            return this.secondNum - that. secondNum;
        } else {
            return this.firstNum - that. firstNum;
        }
    }
}
