package top.theonly.spark.jav.core.accumulator;

import org.apache.spark.util.AccumulatorV2;

/**
 * 自定义累加器
 * @author theonly
 */
public class MyAccumulator extends AccumulatorV2<PersonInfo, PersonInfo> {

    PersonInfo personInfo = new PersonInfo(0, 0, 0, 0);

    /**
     * 要与reset方法中的各个参数的初始值比较
     * @return
     */
    @Override
    public boolean isZero() {
        return this.personInfo.getPersonCount() == 0
                && this.personInfo.getAgeCount() == 0
                && this.personInfo.getMaleCount() == 0
                && this.personInfo.getFemaleCount() == 0;
    }

    /**
     * 从Driver端将自定义的累加器复制到Worker端
     * @return
     */
    @Override
    public AccumulatorV2<PersonInfo, PersonInfo> copy() {
        MyAccumulator accumulator = new MyAccumulator();
        accumulator.personInfo = this.personInfo;
        return accumulator;
    }

    /**
     * 累加器的重置（初始值）
     */
    @Override
    public void reset() {
        this.personInfo = new PersonInfo(0, 0, 0, 0);
    }

    /**
     * 累加器值的相加，作用在Executor端
     * @param v
     */
    @Override
    public void add(PersonInfo v) {
        personInfo.setPersonCount(personInfo.getPersonCount() + v.getPersonCount());
        personInfo.setAgeCount(personInfo.getAgeCount() + v.getAgeCount());
        personInfo.setMaleCount(personInfo.getMaleCount() + v.getMaleCount());
        personInfo.setFemaleCount(personInfo.getFemaleCount() + v.getFemaleCount());
    }

    /**
     * 各个分区的累加器的值的合并
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<PersonInfo, PersonInfo> other) {
        // 两种方式都可以
//        PersonInfo v = ((MyAccumulator) other).personInfo;
        PersonInfo v = other.value();
        personInfo.setPersonCount(personInfo.getPersonCount() + v.getPersonCount());
        personInfo.setAgeCount(personInfo.getAgeCount() + v.getAgeCount());
        personInfo.setMaleCount(personInfo.getMaleCount() + v.getMaleCount());
        personInfo.setFemaleCount(personInfo.getFemaleCount() + v.getFemaleCount());
    }

    /**
     * 获取累加器的值
     * @return
     */
    @Override
    public PersonInfo value() {
        return personInfo;
    }
}
