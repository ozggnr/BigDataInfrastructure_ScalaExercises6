import org.apache.spark.sql.SparkSession

object insuranceEx {
  def main(args: Array[String]) {
    val spark =
      SparkSession
        .builder()
        .master("local[2]")
        .appName("Dataset-Basic")
        .getOrCreate()
    val data = spark.read.option("header",true).csv("/Users/ozgecaylioglu/Desktop/insurance.csv")
    //data.show()
    val dataDf = data.toDF()
    //dataDf.show()
    print((dataDf.count(), dataDf.columns.length))
    dataDf.createOrReplaceTempView("insurance")
    spark.sql("SELECT sex FROM insurance").collect().foreach(println)
    spark.sql("SELECT sex,COUNT(sex) FROM insurance GROUP BY sex").collect().foreach(println)
    spark.sql("SELECT sex,COUNT(sex) FROM insurance WHERE smoker='yes' GROUP BY sex ").collect().foreach(println)
    spark.sql("SELECT region,SUM(charges) as sum_charge FROM insurance GROUP BY region ORDER BY sum_charge DESC").collect().foreach(println)

  }
}
