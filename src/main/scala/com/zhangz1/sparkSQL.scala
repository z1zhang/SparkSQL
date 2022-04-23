package com.zhangz1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
 * Created by zhangz1 on 2022/4/6 19:59
 */
object sparkSQL {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建spark配置对象
    //设置Spark的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    //生成DataFrame对象
    val bikeDF = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm")
      .load("src/main/resources/mobike_shanghai_sample_updated.csv")
    //展示数据
    bikeDF.show()
    //摘要统计
    bikeDF.summary().show()
    //查看数据类型
    bikeDF.printSchema()

    //3.根据起始时间统计周一至周日每天的骑行次数
    val day_count = bikeDF.select(dayofweek(col("start_time")).alias("day")).groupBy("day").count().withColumn("day",
      when(col("day") === 1, "周日").otherwise(
        when(col("day") === 2, "周一").otherwise(
          when(col("day") === 3, "周二").otherwise(
            when(col("day") === 4, "周三").otherwise(
              when(col("day") === 5, "周四").otherwise(
                when(col("day") === 6, "周五").otherwise(
                  when(col("day") === 7, "周六").otherwise("未知"))))))))
    day_count.show()

    //4.根据起始时间统计一天中每个小时的骑行次数并进行降序排列
    val hour_count = bikeDF.select(hour(col("start_time")).alias("hour")).groupBy("hour").count().withColumn("hour",
      when(col("hour") === 0, "0点").otherwise(
        when(col("hour") === 1, "1点").otherwise(
          when(col("hour") === 2, "2点").otherwise(
            when(col("hour") === 3, "3点").otherwise(
              when(col("hour") === 4, "4点").otherwise(
                when(col("hour") === 5, "5点").otherwise(
                  when(col("hour") === 6, "6点").otherwise(
                    when(col("hour") === 7, "7点").otherwise(
                      when(col("hour") === 8, "8点").otherwise(
                        when(col("hour") === 9, "9点").otherwise(
                          when(col("hour") === 10, "10点").otherwise(
                            when(col("hour") === 11, "11点").otherwise(
                              when(col("hour") === 12, "12点").otherwise(
                                when(col("hour") === 13, "13点").otherwise(
                                  when(col("hour") === 14, "14点").otherwise(
                                    when(col("hour") === 15, "15点").otherwise(
                                      when(col("hour") === 16, "16点").otherwise(
                                        when(col("hour") === 17, "17点").otherwise(
                                          when(col("hour") === 18, "18点").otherwise(
                                            when(col("hour") === 19, "19点").otherwise(
                                              when(col("hour") === 20, "20点").otherwise(
                                                when(col("hour") === 21, "21点").otherwise(
                                                  when(col("hour") === 22, "22点").otherwise(
                                                    when(col("hour") === 23, "23点").otherwise("未知")))))))))))))))))))))))))
    hour_count.sort(col("count").desc).show()
    //5.根据起始时间计算骑行时长，命名为riding_time列
    val riding_time = bikeDF.withColumn("riding_time", (unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm") - unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm"))./(60)).select("riding_time")
    riding_time.summary().show()

    //6.根据起始时间统计早晚高峰和平峰的骑行次数
    val morn_even_peak = bikeDF.select(hour(col("start_time")).alias("hour")).withColumn("hour",
      when(col("hour").>(6).&&(col("hour").<=(8)), "早高峰")
        .otherwise(when(col("hour").>(17).&&(col("hour").<=(20)), "晚高峰")
          .otherwise("平峰"))).groupBy("hour").count()
    morn_even_peak.show()

    //7.用户分级（RFM模型）
    //R值：即每个用户最后一次租赁共享单车的时间距9月1日多少天
    val last_rent_day = bikeDF.select(col("userid").alias("r"), datediff(lit("2016-09-01 00:00"), col("start_time"))
      .alias("last_rent_day")).groupBy("r").agg(min("last_rent_day")
      .alias("r_value"))
    //F值：即每个用户累计租赁单车频次
    val rent_count = bikeDF.select(col("userid").alias("f")).groupBy("f").agg(count("f")
      .alias("f_value"))
    //M值：即每个用户累积消费金额
    val total_cost = bikeDF.select(col("userid").alias("m"), (unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm") - unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm"))./(60)
      .alias("total_cost")).withColumn("total_cost",
      when(col("total_cost").%(30).=!=(0), col("total_cost")./(30) + 1)
        .otherwise(col("total_cost")./(30))
        .alias("total_cost")).groupBy("m").agg(sum("total_cost").alias("m_value")).select(col("m"), round(col("m_value"), 2).alias("m_value"))
    //合并到一个DataFrame中
    val rfm = last_rent_day.join(rent_count, last_rent_day("r") === rent_count("f")).join(total_cost, last_rent_day("r") === total_cost("m"))
      .select(last_rent_day("r").alias("userid"), last_rent_day("r_value"), rent_count("f_value"), total_cost("m_value"))
    //计算R分数
    val rfm_score = rfm.withColumn("r_score",
      when(col("r_value").>(14), 1)
        .otherwise(when(col("r_value").>(7).&&(col("r_value").<=(14)), 2)
          .otherwise(when(col("r_value").>(3).&&(col("r_value").<=(7)), 3)
            .otherwise(when(col("r_value").>(1).&&(col("r_value").<=(3)), 4)
              .otherwise(when(col("r_value").>(0).&&(col("r_value").<=(1)), 5)
                .otherwise(1))))))
      //计算F分数
      .withColumn("f_score",
        when(col("f_value").>(20), 5)
          .otherwise(when(col("f_value").>(15).&&(col("f_value").<=(20)), 4)
            .otherwise(when(col("f_value").>(10).&&(col("f_value").<=(15)), 3)
              .otherwise(when(col("f_value").>(5).&&(col("f_value").<=(10)), 2)
                .otherwise(when(col("f_value").>(0).&&(col("f_value").<=(5)), 1)
                  .otherwise(1))))))
      //计算M分数
      .withColumn("m_score",
        when(col("m_value").>(100), 5)
          .otherwise(when(col("m_value").>(60).&&(col("m_value").<=(100)), 4)
            .otherwise(when(col("m_value").>(30).&&(col("m_value").<=(60)), 3)
              .otherwise(when(col("m_value").>(10).&&(col("m_value").<=(30)), 2)
                .otherwise(when(col("m_value").>(0).&&(col("m_value").<=(10)), 1)
                  .otherwise(1))))))
      //RFM值和RFM平均值对比
      .withColumn("R是否大于均值", when(col("r_score").>(3.50), 1).otherwise(0))
      .withColumn("F是否大于均值", when(col("f_score").>(1.64), 1).otherwise(0))
      .withColumn("M是否大于均值", when(col("m_score").>(1.41), 1).otherwise(0))
      .withColumn("RFM分数", col("R是否大于均值").*(100).+(col("F是否大于均值").*(10)).+(col("M是否大于均值").*(1)))
      .withColumn("用户类型", when(col("RFM分数").===(111), "重要价值用户")
        .when(col("RFM分数").===(110), "消费潜力用户")
        .when(col("RFM分数").===(101), "频次深耕用户")
        .when(col("RFM分数").===(100), "新用户")
        .when(col("RFM分数").===(11), "重要价值流失预警用户")
        .when(col("RFM分数").===(10), "一般用户")
        .when(col("RFM分数").===(1), "高消费唤回用户")
        .when(col("RFM分数").===(0), "流失用户"))
    rfm_score.show()
    //8.将结果导出csv文件
    rfm.coalesce(1).write.format("csv").option("header", "true").save("src/main/scala/com/zhangz1/rfm_result.csv")

  }
}