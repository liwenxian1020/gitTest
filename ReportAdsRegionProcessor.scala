package cn.itcast.dmp.report

import cn.itcast.dmp.config.AppConfigHelper
import cn.itcast.dmp.process.ReportProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

/**
  * 报表统计：广告投放的地域分布
  */
object ReportAdsRegionProcessor extends ReportProcessor{
	/**
	  * 提供一个目标表名出去
	  */
	override def targetTableName() = AppConfigHelper.REPORT_ADS_REGION_TABLE_NAME

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

		// 导入SparkSQL函数库
		import org.apache.spark.sql.functions._
		// 导入SparkSession中隐式转换
		import odsDF.sparkSession.implicits._

		// 按照 "province", "city"区域字段分组统计
		val reportDF: DataFrame = odsDF
			.groupBy($"province", $"city")
			.agg(
				sum(
					when(
						$"requestmode".equalTo(1).and($"processnode".geq(1)), 1
					).otherwise(0)
				).as("orginal_req_cnt"),
				sum(
					when(
						$"requestmode".equalTo(1).and($"processnode".geq(2)), 1
					).otherwise(0)
				).as("valid_req_cnt"),
				sum(
					when(
						$"requestmode".equalTo(1).and($"processnode".equalTo(3)), 1
					).otherwise(0)
				).as("ad_req_cnt"),
				sum(
					when(
						$"adplatformproviderid".geq(100000)
							.and($"iseffective".equalTo(1))
							.and($"isbilling".equalTo(1)).and($"isbid".equalTo(1))
							.and($"adorderid".notEqual(0)), 1
					).otherwise(0)
				).as("join_rtx_cnt"),
				sum(
					when(
						$"adplatformproviderid".geq(100000)
							.and($"iseffective".equalTo(1))
							.and($"isbilling".equalTo(1)).and($"iswin".equalTo(1)), 1
					).otherwise(0)
				).as("success_rtx_cnt"),
				sum(
					when(
						$"requestmode".equalTo(2).and($"iseffective".equalTo(1)), 1
					).otherwise(0)
				).as("ad_show_cnt"),
				sum(
					when(
						$"requestmode".equalTo(3).and($"iseffective".equalTo(1)), 1
					).otherwise(0)
				).as("ad_click_cnt"),
				sum(
					when(
						$"requestmode".equalTo(2).and($"iseffective".equalTo(1))
							.and($"isbilling".equalTo(1)), 1
					).otherwise(0)
				).as("media_show_cnt"),
				sum(
					when(
						$"requestmode".equalTo(3).and($"iseffective".equalTo(1))
							.and($"isbilling".equalTo(1)), 1
					).otherwise(0)
				).as("media_click_cnt"),
				sum(
					when(
						$"adplatformproviderid".geq(100000)
							.and($"iseffective".equalTo(1))
							.and($"isbilling".equalTo(1)).and($"iswin".equalTo(1))
							.and($"adorderid".gt(200000)).and($"adcreativeid".gt(200000)),
						floor($"winprice") / 1000
					).otherwise(0)
				).as("dsp_pay_money"),
				sum(
					when(
						$"adplatformproviderid".geq(100000)
							.and($"iseffective".equalTo(1))
							.and($"isbilling".equalTo(1)).and($"iswin".equalTo(1))
							.and($"adorderid".gt(200000)).and($"adcreativeid".gt(200000)),
						floor($"adpayment") / 1000
					).otherwise(0)
				).as("dsp_cost_money")
			)
			.filter(
				$"join_rtx_cnt".notEqual(0).and($"success_rtx_cnt".notEqual(0))
					.and($"ad_show_cnt".notEqual(0)).and($"ad_click_cnt".notEqual(0))
					.and($"media_click_cnt".notEqual(0)).and($"media_click_cnt".notEqual(0))
			)
			.select(
				current_date().cast(StringType).as("report_date"), $"*",
				round($"join_rtx_cnt" / $"success_rtx_cnt", 2).as("success_rtx_rate"),
				round($"ad_click_cnt" / $"ad_show_cnt", 2).as("ad_click_rate"),
				round($"media_click_cnt" / $"media_show_cnt", 2).as("media_click_rate")
			)
		// 返回结构DataFrame
		reportDF
	}


	/**
	  * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
	  *
	  * @param odsDF Kudu中ODS表
	  */
	def realProcessDataWithSQL(odsDF: DataFrame) = {

		// a. 注册DataFrame为临时视图
		odsDF.createOrReplaceTempView("view_tmp_ods")
		// b. 编写SQL
		val reportDF: DataFrame = odsDF.sparkSession.sql(
			//ReportSQLConstant.reportAdsRegionSQL("view_tmp_ods")
			ReportSQLConstant.reportAdsRegionKpiWithSQL("view_tmp_ods")
		)

		// c. 返回结果
		reportDF
	}
}
