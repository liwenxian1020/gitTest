package cn.itcast.dmp.report

import cn.itcast.dmp.config.AppConfigHelper
import cn.itcast.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 各地域数量分布报表统计
  */
object ReportRegionStatProcessor {

	def main(args: Array[String]): Unit = {

		// 第一、创建SparkSession
		val spark: SparkSession = SparkSessionUtils.createSparkSession(this.getClass)
		import spark.implicits._
		import org.apache.spark.sql.functions._

		// 第二、加载ods表的数据
		import cn.itcast.dmp.utils.KuduUtils._
		val optionDF: Option[DataFrame] = spark.readKuduTable(AppConfigHelper.AD_MAIN_TABLE_NAME)
		val odsDF: DataFrame = optionDF match {
			case Some(dataframe) => dataframe
			case None => println("ERROR: ODS表无数据,结束执行"); return
		}

		// 第三、按照省市分组，统计结果：编写SQL分析
		// a. 注册DataFrame为临时视图
		odsDF.createOrReplaceTempView("view_tmp_ods")
		// b. 编写SQL
		val reportDF: DataFrame = spark.sql(
			"""
			  |SELECT
			  |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
			  |  province, city, COUNT(1) AS count
			  |FROM
			  |  view_tmp_ods
			  |GROUP BY
			  |  province, city
			""".stripMargin)

		// TODO: 使用DataFrame中DSL编程实现
		val reportDF2 = odsDF
	    	.groupBy($"province", $"city").count()
	    	.select(
				current_date().cast(StringType).as("report_date"),
				$"province", $"city", $"count"
			)
	    	.sort($"count".desc)
		reportDF2.show(10, truncate = false)

		// 第四、结果数据保存到Kudu表中
		val reportTableName = AppConfigHelper.REPORT_REGION_STAT_TABLE_NAME
		// a. 当表不存在时，创建表
		spark.createKuduTable(
			reportTableName, reportDF.schema,
			Seq("report_date", "province", "city"), isDelete = false
		)
		// b. 保存数据
		reportDF.saveAsKuduTable(reportTableName)


		// 应用结束，关闭资源
		spark.stop()
	}

}
