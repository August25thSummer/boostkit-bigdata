/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, Row}

class ColumnarUnionExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var left: DataFrame = _
  private var right: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    left = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", "", 4, 2.0),
      ("", "Hello", 1, 1.0),
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")

    right = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      (null, "", 4, 2.0),
      (null, null, 2, 1.0),
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar union exec happened") {
    val res = left.union(right)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined, s"ColumnarUnionExec not happened, executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar union is equal to expected") {
    val expected = Array(Row("abc", "", 4, 2.0),
      Row("", "Hello", 1, 1.0),
      Row(" add", "World", 8, 3.0),
      Row(" yeah  ", "yeah", 10, 8.0),
      Row(null, "", 4, 2.0),
      Row(null, null, 1, 1.0),
      Row(" add", "World", 8, 3.0),
      Row(" yeah  ", "yeah", 10, 8.0))
    val res = left.union(right)
    val result: Array[Row] = res.head(8)
    assertResult(expected)(result)
  }

  test("columnar union is equal to native with null") {
    val df = left.union(right)
    val children = Seq(left.queryExecution.executedPlan, right.queryExecution.executedPlan)
    checkThatPlansAgreeTemplate(df, children)
  }

  def checkThatPlansAgreeTemplate(df: DataFrame, child: Seq[SparkPlan]): Unit = {
    checkThatPlansAgree(
      df,
      (_: SparkPlan) =>
        ColumnarUnionExec(child),
      (_: SparkPlan) =>
        UnionExec(child),
      sortAnswers = false)
  }
}
