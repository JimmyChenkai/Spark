/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.parser

import java.util

import scala.collection.mutable.StringBuilder

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

/**
 * 在解析过程中，工具类
 */
object ParserUtils {
  //创建获取token的命令
  def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
  /**
  * 非法操作异常处理类
  * @param 传入一段信息和解析规则
  * @return 抛出解析非法操作异常“Opration not allowed”+具体信息
  **/
  def operationNotAllowed(message: String, ctx: ParserRuleContext): Nothing = {
    throw new ParseException(s"Operation not allowed: $message", ctx)
  }
  
  /**
  * 校验多个表示异常处理类
  * @param 节点集合，表达式，解析规则
  * @return 抛出解析多表达式异常“Found duplicate clauses:”+具体表达式
  **/
  def checkDuplicateClauses[T](
      nodes: util.List[T], clauseName: String, ctx: ParserRuleContext): Unit = {
    if (nodes.size() > 1) {
      throw new ParseException(s"Found duplicate clauses: $clauseName", ctx)
    }
  }

  /** 
  * 校验是否包含多个重复的key  比如AstBuilder中withCTE中在生成LogicalPlan前做了校验
  * @param 一组KEY-VALUE集合，解析规则
  * @return 抛出解析重复key异常“Found duplicate keys”+key
  */  
  def checkDuplicateKeys[T](keyPairs: Seq[(String, T)], ctx: ParserRuleContext): Unit = {
    keyPairs.groupBy(_._1).filter(_._2.size > 1).foreach { case (key, _) =>
      throw new ParseException(s"Found duplicate keys '$key'.", ctx)
    }
  }

  /**
  * 根据已有的节点，创建指定代码
  * @param 创建接续规则上下文
  * @return 根据解析规则上下文开始index和结束index，创建stream代码
  */
  def source(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(ctx.getStart.getStartIndex, ctx.getStop.getStopIndex))
  }

  /** Get all the text which comes after the given rule. 
  *  根据已有的节点，创建所有文本
  * @param 解析规则上下文
  * @return 上下文，具体逻辑直接调用remainder方法，详细看remainder方式
  */
  def remainder(ctx: ParserRuleContext): String = remainder(ctx.getStop)

  /** Get all the text which comes after the given token. 
  * remainder具体逻辑代码
  * @param token
  * @return code
  */
  def remainder(token: Token): String = {
    val stream = token.getInputStream
    val interval = Interval.of(token.getStopIndex + 1, stream.size() - 1)
    stream.getText(interval)
  }

  /** Convert a string token into a string. 
  * 字符串类型的token转换为scala字符串
  * @param Token
  * @return 对应字符串  核心看unescapeSQLString方法
  */
  def string(token: Token): String = unescapeSQLString(token.getText)

  /** Convert a string node into a string. 
  * 字符串类型node转为scala字符串
  * @param 字符串类型TerminalNode
  * @return 返回对应字符串  核心看unescapeSQLString方法
  */
  def string(node: TerminalNode): String = unescapeSQLString(node.getText)

  /** Convert a string node into a string without unescaping. 
  * 把字符串类型node转为非修饰的字符创,方法描述也很详细，就是解析规则强制要求输入一定要在开始结束之间要有引用标识
  * @param TerninalNode
  * @return String
  */
  def stringWithoutUnescape(node: TerminalNode): String = {
    // STRING parser rule forces that the input always has quotes at the starting and ending.
    node.getText.slice(1, node.getText.size - 1)
  }

  /** Get the origin (line and position) of the token. 
  * 获取一个token的位置信息
  * @parma Token
  * @return Origin
  */
  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Validate the condition. If it doesn't throw a parse exception. 
  * 这个就比较简单的校验条件是否合法，如果不合法就抛出解析异常
  */
  def validate(f: => Boolean, message: String, ctx: ParserRuleContext): Unit = {
    if (!f) {
      throw new ParseException(message, ctx)
    }
  }

  /**
   * Register the origin of the context. Any TreeNode created in the closure will be assigned the
   * registered origin. This method restores the previously set origin after completion of the
   * closure.
   * 注册解析规则上下文的来源。
   * 在闭环中创建的任何树节点都将被指定为已注册的源节点。
   * 此方法在关闭完成后恢复先前设置的原点。
   * @param ParseRuleContext
   * @return 泛型
   */
  def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  /** Unescape baskslash-escaped string enclosed by quotes. 
  * 非修饰
  */
  def unescapeSQLString(b: String): String = {
    var enclosure: Character = null
    val sb = new StringBuilder(b.length())
    //字符转换并赋值到sb数组中
    def appendEscapedChar(n: Char) {
      n match {
        case '0' => sb.append('\u0000')
        case '\'' => sb.append('\'')
        case '"' => sb.append('\"')
        case 'b' => sb.append('\b')
        case 'n' => sb.append('\n')
        case 'r' => sb.append('\r')
        case 't' => sb.append('\t')
        case 'Z' => sb.append('\u001A')
        case '\\' => sb.append('\\')
        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
        case '%' => sb.append("\\%")
        case '_' => sb.append("\\_")
        case _ => sb.append(n)
      }
    }

    var i = 0
    val strLength = b.length
    while (i < strLength) {
      val currentChar = b.charAt(i)
      if (enclosure == null) {//若enclosure为空，同时currentChar是'\'、'\"' 就赋值给enclosure
        if (currentChar == '\'' || currentChar == '\"') {
          enclosure = currentChar
        }
      } else if (enclosure == currentChar) {//若相等enclosure置空
        enclosure = null
      } else if (currentChar == '\\') {//若currentChar是'\\'

        if ((i + 6 < strLength) && b.charAt(i + 1) == 'u') {//'\u0000'处理
          // \u0000 style character literals.

          val base = i + 2
          val code = (0 until 4).foldLeft(0) { (mid, j) =>
            val digit = Character.digit(b.charAt(j + base), 16)
            (mid << 4) + digit
          }
          sb.append(code.asInstanceOf[Char])
          i += 5
        } else if (i + 4 < strLength) {//'\000'处理
          // \000 style character literals.

          val i1 = b.charAt(i + 1)
          val i2 = b.charAt(i + 2)
          val i3 = b.charAt(i + 3)

          if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7') && (i3 >= '0' && i3 <= '7')) {
            val tmp = ((i3 - '0') + ((i2 - '0') << 3) + ((i1 - '0') << 6)).asInstanceOf[Char]
            sb.append(tmp)
            i += 3
          } else {
            appendEscapedChar(i1)
            i += 1
          }
        } else if (i + 2 < strLength) {
          // escaped character literals.
          val n = b.charAt(i + 1)
          appendEscapedChar(n)
          i += 1
        }
      } else {
        // non-escaped character literals.
        sb.append(currentChar)
      }
      i += 1
    }
    sb.toString()
  }

  /** 不带限定符的带引号regex中的列名模式 */
  val escapedIdentifier = "`(.+)`".r

  /** 带限定符的带引号regex中的列名模式*/
  val qualifiedEscapedIdentifier = ("(.+)" + """.""" + "`(.+)`").r

  /** 一些语法糖，使逻辑计划的可选从句更容易使用 */
  implicit class EnhancedLogicalPlan(val plan: LogicalPlan) extends AnyVal {
    /**
     *当给定的上下文存在时，使用代码块创建计划。否则返回原始计划
     */
    def optional(ctx: AnyRef)(f: => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f
      } else {
        plan
      }
    }

    /**
     * 逻辑计划转换，若旧的上下文解析规则存在就使用旧的方法，否则就返回原始计划
     */
    def optionalMap[C](ctx: C)(f: (C, LogicalPlan) => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f(ctx, plan)
      } else {
        plan
      }
    }
  }
}
