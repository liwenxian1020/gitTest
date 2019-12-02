package cn.itcast.dmp.report

import cn.itcast.dmp.config.AppConfigHelper
import cn.itcast.dmp.process.ReportProcessor
import org.apache.spark.sql.DataFrame

/**
  * 报表统计：各地域数量分布报表统计
  */
object ReportRegionProcessor extends ReportProcessor {
	/**
	  * 提供一个目标表名出去
	  */
	override def targetTableName() = AppConfigHelper.REPORT_REGION_STAT_TABLE_NAME

	/**
	  * 提供目标表的分区键
	  */
	override def targetTableKeys() = Seq("report_date", "province", "city")

	/**
	  * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
	  *
	  * @param odsDF Kudu中ODS表
	  */
	override def realProcessData(odsDF: DataFrame) = {
		// a. 注册DataFrame为临时视图
		odsDF.createOrReplaceTempView("view_tmp_ods")
		// b. 编写SQL
		val reportDF: DataFrame = odsDF.sparkSession.sql(
			"""
			  |SELECT
			  |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
			  |  province, city, COUNT(1) AS count
			  |FROM
			  |  view_tmp_ods
			  |GROUP BY
			  |  province, city
			""".stripMargin)

		// c. 返回结果
		reportDF
	}
}
