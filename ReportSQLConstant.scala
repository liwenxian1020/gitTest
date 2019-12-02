package cn.itcast.dmp.report

/**
  * 统计报表的SQL语句
  */
object ReportSQLConstant {

	/**
	  * 广告投放的地域分布的SQL语句
	  *
	  * @param tempViewName DataFrame注册的临时视图名称
	  */
	def reportAdsKpiWithSQL(tempViewName: String, fieldGroups: Seq[String]): String = {
		// 依据分组字段，构建 语句
		val fieldStr = fieldGroups.map(field => s"t.$field").mkString(",")
		val fieldStr2 = fieldGroups.mkString(",")

		s"""
		   |WITH temp AS(
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  $fieldStr,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |               AND t.processnode >= 1) THEN 1
		   |        ELSE 0
		   |      END) AS orginal_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |              AND t.processnode >= 2) THEN 1
		   |        ELSE 0
		   |      END) AS valid_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |              AND t.processnode = 3) THEN 1
		   |        ELSE 0
		   |      END) AS ad_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.adplatformproviderid >= 100000
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1
		   |              AND t.isbid = 1
		   |              AND t.adorderid != 0) THEN 1
		   |          ELSE 0
		   |        END) AS join_rtx_cnt,
		   |  sum(CASE
		   |        WHEN (t.adplatformproviderid >= 100000
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1
		   |              AND t.iswin = 1) THEN 1
		   |          ELSE 0
		   |      END) AS success_rtx_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 2
		   |              AND t.iseffective = 1) THEN 1
		   |        ELSE 0
		   |      END) AS ad_show_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 3
		   |              AND t.iseffective = 1) THEN 1
		   |        ELSE 0
		   |      END) AS ad_click_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 2
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1) THEN 1
		   |        ELSE 0
		   |      END) AS media_show_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 3
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1) THEN 1
		   |          ELSE 0
		   |       END) AS media_click_cnt,
		   |  sum(CASE
		   |          WHEN (t.adplatformproviderid >= 100000
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1
		   |                AND t.iswin = 1
		   |                AND t.adorderid > 200000
		   |                AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
		   |          ELSE 0
		   |      END) AS dsp_pay_money,
		   |  sum(CASE
		   |          WHEN (t.adplatformproviderid >= 100000
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1
		   |                AND t.iswin = 1
		   |                AND t.adorderid > 200000
		   |                AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
		   |          ELSE 0
		   |      END) AS dsp_cost_money
		   |FROM
		   |  $tempViewName t
		   |GROUP BY
		   |  $fieldStr
		   |)
		   |SELECT
		   |  report_date, $fieldStr2, orginal_req_cnt, valid_req_cnt,
		   |  ad_req_cnt, join_rtx_cnt, success_rtx_cnt, ad_show_cnt,
		   |  ad_click_cnt, media_show_cnt, media_click_cnt,
		   |  dsp_pay_money, dsp_cost_money,
		   |  round(success_rtx_cnt / join_rtx_cnt, 2) AS success_rtx_rate,
		   |  round(ad_click_cnt / ad_show_cnt, 2) AS ad_click_rate,
		   |  round(media_click_cnt / media_show_cnt, 2) AS media_click_rate
		   |FROM
		   |  temp
		   |WHERE
		   |  join_rtx_cnt != 0 AND success_rtx_cnt != 0
		   |  AND
		   |  ad_show_cnt != 0 AND ad_click_cnt != 0
		   |  AND
		   |  media_show_cnt != 0 AND media_click_cnt != 0
		""".stripMargin
	}

	/**
	  * 广告投放的地域分布的SQL语句
	  *
	  * @param tempViewName DataFrame注册的临时视图名称
	  */
	def reportAdsAppKpiWithSQL(tempViewName: String): String = {
		s"""
		   |WITH temp AS(
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  t.appid,
		   |  t.appname,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |               AND t.processnode >= 1) THEN 1
		   |        ELSE 0
		   |      END) AS orginal_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |              AND t.processnode >= 2) THEN 1
		   |        ELSE 0
		   |      END) AS valid_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |              AND t.processnode = 3) THEN 1
		   |        ELSE 0
		   |      END) AS ad_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.adplatformproviderid >= 100000
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1
		   |              AND t.isbid = 1
		   |              AND t.adorderid != 0) THEN 1
		   |          ELSE 0
		   |        END) AS join_rtx_cnt,
		   |  sum(CASE
		   |        WHEN (t.adplatformproviderid >= 100000
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1
		   |              AND t.iswin = 1) THEN 1
		   |          ELSE 0
		   |      END) AS success_rtx_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 2
		   |              AND t.iseffective = 1) THEN 1
		   |        ELSE 0
		   |      END) AS ad_show_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 3
		   |              AND t.iseffective = 1) THEN 1
		   |        ELSE 0
		   |      END) AS ad_click_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 2
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1) THEN 1
		   |        ELSE 0
		   |      END) AS media_show_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 3
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1) THEN 1
		   |          ELSE 0
		   |       END) AS media_click_cnt,
		   |  sum(CASE
		   |          WHEN (t.adplatformproviderid >= 100000
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1
		   |                AND t.iswin = 1
		   |                AND t.adorderid > 200000
		   |                AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
		   |          ELSE 0
		   |      END) AS dsp_pay_money,
		   |  sum(CASE
		   |          WHEN (t.adplatformproviderid >= 100000
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1
		   |                AND t.iswin = 1
		   |                AND t.adorderid > 200000
		   |                AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
		   |          ELSE 0
		   |      END) AS dsp_cost_money
		   |FROM
		   |  $tempViewName t
		   |GROUP BY
		   |  t.appid, t.appname
		   |)
		   |SELECT
		   |  report_date, appid, appname, orginal_req_cnt, valid_req_cnt,
		   |  ad_req_cnt, join_rtx_cnt, success_rtx_cnt, ad_show_cnt,
		   |  ad_click_cnt, media_show_cnt, media_click_cnt,
		   |  dsp_pay_money, dsp_cost_money,
		   |  round(success_rtx_cnt / join_rtx_cnt, 2) AS success_rtx_rate,
		   |  round(ad_click_cnt / ad_show_cnt, 2) AS ad_click_rate,
		   |  round(media_click_cnt / media_show_cnt, 2) AS media_click_rate
		   |FROM
		   |  temp
		   |WHERE
		   |  join_rtx_cnt != 0 AND success_rtx_cnt != 0
		   |  AND
		   |  ad_show_cnt != 0 AND ad_click_cnt != 0
		   |  AND
		   |  media_show_cnt != 0 AND media_click_cnt != 0
		""".stripMargin
	}

	/**
	  * 广告投放的地域分布的SQL语句
	  *
	  * @param tempViewName DataFrame注册的临时视图名称
	  */
	def reportAdsRegionKpiWithSQL(tempViewName: String): String = {
		s"""
		   |WITH temp AS(
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  t.province,
		   |  t.city,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |               AND t.processnode >= 1) THEN 1
		   |        ELSE 0
		   |      END) AS orginal_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |              AND t.processnode >= 2) THEN 1
		   |        ELSE 0
		   |      END) AS valid_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 1
		   |              AND t.processnode = 3) THEN 1
		   |        ELSE 0
		   |      END) AS ad_req_cnt,
		   |  sum(CASE
		   |        WHEN (t.adplatformproviderid >= 100000
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1
		   |              AND t.isbid = 1
		   |              AND t.adorderid != 0) THEN 1
		   |          ELSE 0
		   |        END) AS join_rtx_cnt,
		   |  sum(CASE
		   |        WHEN (t.adplatformproviderid >= 100000
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1
		   |              AND t.iswin = 1) THEN 1
		   |          ELSE 0
		   |      END) AS success_rtx_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 2
		   |              AND t.iseffective = 1) THEN 1
		   |        ELSE 0
		   |      END) AS ad_show_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 3
		   |              AND t.iseffective = 1) THEN 1
		   |        ELSE 0
		   |      END) AS ad_click_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 2
		   |              AND t.iseffective = 1
		   |              AND t.isbilling = 1) THEN 1
		   |        ELSE 0
		   |      END) AS media_show_cnt,
		   |  sum(CASE
		   |        WHEN (t.requestmode = 3
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1) THEN 1
		   |          ELSE 0
		   |       END) AS media_click_cnt,
		   |  sum(CASE
		   |          WHEN (t.adplatformproviderid >= 100000
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1
		   |                AND t.iswin = 1
		   |                AND t.adorderid > 200000
		   |                AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
		   |          ELSE 0
		   |      END) AS dsp_pay_money,
		   |  sum(CASE
		   |          WHEN (t.adplatformproviderid >= 100000
		   |                AND t.iseffective = 1
		   |                AND t.isbilling = 1
		   |                AND t.iswin = 1
		   |                AND t.adorderid > 200000
		   |                AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
		   |          ELSE 0
		   |      END) AS dsp_cost_money
		   |FROM
		   |  $tempViewName t
		   |GROUP BY
		   |  t.province, t.city
		   |)
		   |SELECT
		   |  report_date, province, city, orginal_req_cnt, valid_req_cnt,
		   |  ad_req_cnt, join_rtx_cnt, success_rtx_cnt, ad_show_cnt,
		   |  ad_click_cnt, media_show_cnt, media_click_cnt,
		   |  dsp_pay_money, dsp_cost_money,
		   |  round(success_rtx_cnt / join_rtx_cnt, 2) AS success_rtx_rate,
		   |  round(ad_click_cnt / ad_show_cnt, 2) AS ad_click_rate,
		   |  round(media_click_cnt / media_show_cnt, 2) AS media_click_rate
		   |FROM
		   |  temp
		   |WHERE
		   |  join_rtx_cnt != 0 AND success_rtx_cnt != 0
		   |  AND
		   |  ad_show_cnt != 0 AND ad_click_cnt != 0
		   |  AND
		   |  media_show_cnt != 0 AND media_click_cnt != 0
		""".stripMargin
	}



	/**
	  * 广告投放的地域分布的SQL语句
	  *
	  * @param tempViewName DataFrame注册的临时视图名称
	  */
	def reportAdsRegionSQL(tempViewName: String): String = {
		s"""
		  |SELECT
		  |  tmp.*,
		  |  (tmp.success_rtx_cnt / tmp.join_rtx_cnt) AS success_rtx_rate,
		  |  (tmp.ad_click_cnt / tmp.ad_show_cnt) AS ad_click_rate,
		  |  (tmp.media_click_cnt / tmp.media_show_cnt) AS media_click_rate
		  |FROM(
		  |SELECT
		  |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		  |  province, city,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 1
		  |		      AND processnode >= 1) THEN 1
		  |	    ELSE 0
		  |	  END) AS orginal_req_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 1
		  |		      AND processnode >= 2) THEN 1
		  |	    ELSE 0
		  |	  END) AS valid_req_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 1
		  |		      AND processnode = 3) THEN 1
		  |	    ELSE 0
		  |	  END) AS ad_req_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   adplatformproviderid >= 100000
		  |		      AND iseffective = 1
		  |			  AND isbilling = 1
		  |			  AND isbid = 1
		  |		      AND adorderid != 0) THEN 1
		  |	    ELSE 0
		  |	  END) AS join_rtx_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   adplatformproviderid >= 100000
		  |		      AND iseffective = 1
		  |			  AND isbilling = 1
		  |			  AND iswin = 1
		  |		      AND adorderid != 0) THEN 1
		  |	    ELSE 0
		  |	  END) AS success_rtx_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 2
		  |		      AND iseffective = 1) THEN 1
		  |	    ELSE 0
		  |	  END) AS ad_show_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 3
		  |		      AND iseffective = 1
		  |			  AND adorderid != 0) THEN 1
		  |	    ELSE 0
		  |	  END) AS ad_click_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 2
		  |		      AND iseffective = 1
		  |			  AND isbilling = 1
		  |			  AND isbid = 1
		  |		      AND iswin = 1) THEN 1
		  |	    ELSE 0
		  |	  END) AS media_show_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   requestmode = 3
		  |		      AND iseffective = 1
		  |			  AND isbilling = 1
		  |			  AND isbid = 1
		  |		      AND iswin = 1) THEN 1
		  |	    ELSE 0
		  |	  END) AS media_click_cnt,
		  |  SUM(CASE
		  |        WHEN (
		  |		   adplatformproviderid >= 100000
		  |		      AND iseffective = 1
		  |			  AND isbilling = 1
		  |		      AND iswin = 1
		  |			  AND adorderid > 200000
		  |			  AND adcreativeid > 200000
		  |			  ) THEN floor(winprice / 1000)
		  |	    ELSE 0
		  |	  END) AS dsp_pay_money,
		  |  SUM(CASE
		  |        WHEN (
		  |		   adplatformproviderid >= 100000
		  |		      AND iseffective = 1
		  |			  AND isbilling = 1
		  |			  AND isbid = 1
		  |		      AND iswin = 1
		  |			  AND adorderid > 200000
		  |			  AND adcreativeid > 200000
		  |			  ) THEN floor(adpayment / 1000)
		  |	    ELSE 0
		  |	  END) AS dsp_cost_money
		  |FROM
		  |  $tempViewName
		  |GROUP BY
		  |  province, city
		  |) tmp
		  |WHERE
		  |  tmp.success_rtx_cnt != 0 AND tmp.join_rtx_cnt != 0 AND
		  |  tmp.ad_click_cnt != 0 AND tmp.ad_show_cnt != 0 AND
		  |  tmp.media_click_cnt != 0 AND tmp.media_show_cnt != 0
		""".stripMargin
	}

}
