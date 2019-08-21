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

package org.apache.spark.sql.hive.execution

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Properties
import javax.annotation.Nullable

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{RecordReader, RecordWriter}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.io.Writable

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveInspectors
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{CircularBuffer, RedirectThread, SerializableConfiguration, Utils}

/**
 * 通过分叉和运行指定的脚本来转换输入
 *
 * @param 输入应传递给脚本的表达式集。
 * @param 脚本应该执行的命令。
 * @param 输出脚本生成的属性。
 */
case class ScriptTransformationExec(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: HiveScriptIOSchema)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = outputSet -- inputSet

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = {
    def processIterator(inputIterator: Iterator[InternalRow], hadoopConf: Configuration)
      : Iterator[InternalRow] = {
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd.asJava)

      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val errorStream = proc.getErrorStream

      //为了避免死锁，我们需要使用子进程的错误输出。
      //为避免大错误输出引起的问题，我们使用循环缓冲区来限制数量
      //我们保留的错误输出。有关死锁/挂起的更多讨论，请参见SPARK-7862
      val stderrBuffer = new CircularBuffer(2048)
      new RedirectThread(
        errorStream,
        stderrBuffer,
        "Thread-ScriptTransformation-STDERR-Consumer").start()

      val outputProjection = new InterpretedProjection(input, child.output)

      //这种可为空性是一种性能优化，以避免Option.foreach（）调用在循环内部
      @Nullable val (inputSerde, inputSoi) = ioschema.initInputSerDe(input).getOrElse((null, null))

      //这个新线程将使用ScriptTransformation的输入行并将它们写入
      //外部流程 该进程的输出将由当前线程读取。
      val writerThread = new ScriptTransformationWriterThread(
        inputIterator,
        input.map(_.dataType),
        outputProjection,
        inputSerde,
        inputSoi,
        ioschema,
        outputStream,
        proc,
        stderrBuffer,
        TaskContext.get(),
        hadoopConf
      )

      //这种可为空性是一种性能优化，以避免Option.foreach（）调用在循环内部
      @Nullable val (outputSerde, outputSoi) = {
        ioschema.initOutputSerDe(output).getOrElse((null, null))
      }

      val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
      val outputIterator: Iterator[InternalRow] = new Iterator[InternalRow] with HiveInspectors {
        var curLine: String = null
        val scriptOutputStream = new DataInputStream(inputStream)

        @Nullable val scriptOutputReader =
          ioschema.recordReader(scriptOutputStream, hadoopConf).orNull

        var scriptOutputWritable: Writable = null
        val reusedWritableObject: Writable = if (null != outputSerde) {
          outputSerde.getSerializedClass().getConstructor().newInstance()
        } else {
          null
        }
        val mutableRow = new SpecificInternalRow(output.map(_.dataType))

        @transient
        lazy val unwrappers = outputSoi.getAllStructFieldRefs.asScala.map(unwrapperFor)

        private def checkFailureAndPropagate(cause: Throwable = null): Unit = {
          if (writerThread.exception.isDefined) {
            throw writerThread.exception.get
          }

          if (!proc.isAlive) {
            val exitCode = proc.exitValue()
            if (exitCode != 0) {
              logError(stderrBuffer.toString) // log the stderr circular buffer
              throw new SparkException(s"Subprocess exited with status $exitCode. " +
                s"Error: ${stderrBuffer.toString}", cause)
            }
          }
        }

        override def hasNext: Boolean = {
          try {
            if (outputSerde == null) {
              if (curLine == null) {
                curLine = reader.readLine()
                if (curLine == null) {
                  checkFailureAndPropagate()
                  return false
                }
              }
            } else if (scriptOutputWritable == null) {
              scriptOutputWritable = reusedWritableObject

              if (scriptOutputReader != null) {
                if (scriptOutputReader.next(scriptOutputWritable) <= 0) {
                  checkFailureAndPropagate()
                  return false
                }
              } else {
                try {
                  scriptOutputWritable.readFields(scriptOutputStream)
                } catch {
                  case _: EOFException =>
                    //这意味着`proc`（即TRANSFORM进程）的标准输出已经耗尽。
                    //理想情况下，proc应该*不*在这一点上活着但是
                    // EOF写出和进程之间可能存在延迟
                    //被终止 所以明确地等待进程完成。
                    proc.waitFor()
                    checkFailureAndPropagate()
                    return false
                }
              }
            }

            true
          } catch {
            case NonFatal(e) =>
              //如果此异常是由于`proc`的突然/不干净终止，
              //然后检测它并为最终用户传播更好的异常消息
              checkFailureAndPropagate(e)

              throw e
          }
        }

        override def next(): InternalRow = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          if (outputSerde == null) {
            val prevLine = curLine
            curLine = reader.readLine()
            if (!ioschema.schemaLess) {
              new GenericInternalRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
                  .map(CatalystTypeConverters.convertToCatalyst))
            } else {
              new GenericInternalRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"), 2)
                  .map(CatalystTypeConverters.convertToCatalyst))
            }
          } else {
            val raw = outputSerde.deserialize(scriptOutputWritable)
            scriptOutputWritable = null
            val dataList = outputSoi.getStructFieldsDataAsList(raw)
            var i = 0
            while (i < dataList.size()) {
              if (dataList.get(i) == null) {
                mutableRow.setNullAt(i)
              } else {
                unwrappers(i)(dataList.get(i), mutableRow, i)
              }
              i += 1
            }
            mutableRow
          }
        }
      }

      writerThread.start()

      outputIterator
    }

    val broadcastedHadoopConf =
      new SerializableConfiguration(sqlContext.sessionState.newHadoopConf())

    child.execute().mapPartitions { iter =>
      if (iter.hasNext) {
        val proj = UnsafeProjection.create(schema)
        processIterator(iter, broadcastedHadoopConf.value).map(proj)
      } else {
        //如果输入迭代器没有行，则不要启动外部脚本。
        Iterator.empty
      }
    }
  }
}

private class ScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    outputProjection: Projection,
    @Nullable inputSerde: AbstractSerDe,
    @Nullable inputSoi: ObjectInspector,
    ioschema: HiveScriptIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration
  ) extends Thread("Thread-ScriptTransformation-Feed") with Logging {

  setDaemon(true)

  @volatile private var _exception: Throwable = null

  /** 包含将父迭代器写入外部进程时抛出的异常。 */
  def exception: Option[Throwable] = Option(_exception)

  override def run(): Unit = Utils.logUncaughtExceptions {
    TaskContext.setTaskContext(taskContext)

    val dataOutputStream = new DataOutputStream(outputStream)
    @Nullable val scriptInputWriter = ioschema.recordWriter(dataOutputStream, conf).orNull

    //我们不能在这里使用Utils.tryWithSafeFinally因为我们还需要一个`catch`块，所以
    //让我们使用一个变量来记录`finally`块是否因异常而被命中
    var threwException: Boolean = true
    val len = inputSchema.length
    try {
      iter.map(outputProjection).foreach { row =>
        if (inputSerde == null) {
          val data = if (len == 0) {
            ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES")
          } else {
            val sb = new StringBuilder
            sb.append(row.get(0, inputSchema(0)))
            var i = 1
            while (i < len) {
              sb.append(ioschema.inputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
              sb.append(row.get(i, inputSchema(i)))
              i += 1
            }
            sb.append(ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES"))
            sb.toString()
          }
          outputStream.write(data.getBytes(StandardCharsets.UTF_8))
        } else {
          val writable = inputSerde.serialize(
            row.asInstanceOf[GenericInternalRow].values, inputSoi)

          if (scriptInputWriter != null) {
            scriptInputWriter.write(writable)
          } else {
            prepareWritable(writable, ioschema.outputSerdeProps).write(dataOutputStream)
          }
        }
      }
      threwException = false
    } catch {
      // SPARK-25158不应再次抛出异常，否则将被捕获
      // SparkUncaughtExceptionHandler，然后Executor将因为此Uncaught Exception而退出，
      // 所以将异常传递给`ScriptTransformationExec`就足够了。
      case t: Throwable =>
        // 写入输入时发生错误，因此请终止子进程。根据
        // Javadoc这个调用不会抛出异常：
        _exception = t
        proc.destroy()
        logError("Thread-ScriptTransformation-Feed exit cause by: ", t)
    } finally {
      try {
        Utils.tryLogNonFatalError(outputStream.close())
        if (proc.waitFor() != 0) {
          logError(stderrBuffer.toString) // log the stderr circular buffer
        }
      } catch {
        case NonFatal(exceptionFromFinallyBlock) =>
          if (!threwException) {
            throw exceptionFromFinallyBlock
          } else {
            log.error("Exception in finally block", exceptionFromFinallyBlock)
          }
      }
    }
  }
}

object HiveScriptIOSchema {
  def apply(input: ScriptInputOutputSchema): HiveScriptIOSchema = {
    HiveScriptIOSchema(
      input.inputRowFormat,
      input.outputRowFormat,
      input.inputSerdeClass,
      input.outputSerdeClass,
      input.inputSerdeProps,
      input.outputSerdeProps,
      input.recordReaderClass,
      input.recordWriterClass,
      input.schemaLess)
  }
}

/**
 * Hive输入和输出模式属性的包装类
 */
case class HiveScriptIOSchema (
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: Option[String],
    outputSerdeClass: Option[String],
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReaderClass: Option[String],
    recordWriterClass: Option[String],
    schemaLess: Boolean)
  extends HiveInspectors {

  private val defaultFormat = Map(
    ("TOK_TABLEROWFORMATFIELD", "\t"),
    ("TOK_TABLEROWFORMATLINES", "\n")
  )

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))


  def initInputSerDe(input: Seq[Expression]): Option[(AbstractSerDe, ObjectInspector)] = {
    inputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(input)
      val serde = initSerDe(serdeClass, columns, columnTypes, inputSerdeProps)
      val fieldObjectInspectors = columnTypes.map(toInspector)
      val objectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columns.asJava, fieldObjectInspectors.asJava)
        .asInstanceOf[ObjectInspector]
      (serde, objectInspector)
    }
  }

  def initOutputSerDe(output: Seq[Attribute]): Option[(AbstractSerDe, StructObjectInspector)] = {
    outputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(output)
      val serde = initSerDe(serdeClass, columns, columnTypes, outputSerdeProps)
      val structObjectInspector = serde.getObjectInspector().asInstanceOf[StructObjectInspector]
      (serde, structObjectInspector)
    }
  }

  private def parseAttrs(attrs: Seq[Expression]): (Seq[String], Seq[DataType]) = {
    val columns = attrs.zipWithIndex.map(e => s"${e._1.prettyName}_${e._2}")
    val columnTypes = attrs.map(_.dataType)
    (columns, columnTypes)
  }

  private def initSerDe(
      serdeClassName: String,
      columns: Seq[String],
      columnTypes: Seq[DataType],
      serdeProps: Seq[(String, String)]): AbstractSerDe = {

    val serde = Utils.classForName[AbstractSerDe](serdeClassName).getConstructor().
      newInstance()

    val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")

    var propsMap = serdeProps.toMap + (serdeConstants.LIST_COLUMNS -> columns.mkString(","))
    propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypesNames)

    val properties = new Properties()
    //不能在scala-2.12中使用properties.putAll（propsMap.asJava）
    //请参阅https://github.com/scala/bug/issues/10418
    propsMap.foreach { case (k, v) => properties.put(k, v) }
    serde.initialize(null, properties)

    serde
  }

  def recordReader(
      inputStream: InputStream,
      conf: Configuration): Option[RecordReader] = {
    recordReaderClass.map { klass =>
      val instance = Utils.classForName[RecordReader](klass).getConstructor().
        newInstance()
      val props = new Properties()
      //不能在scala-2.12中使用props.putAll（outputSerdeProps.toMap.asJava）
      //请参阅https://github.com/scala/bug/issues/10418
      outputSerdeProps.toMap.foreach { case (k, v) => props.put(k, v) }
      instance.initialize(inputStream, conf, props)
      instance
    }
  }

  def recordWriter(outputStream: OutputStream, conf: Configuration): Option[RecordWriter] = {
    recordWriterClass.map { klass =>
      val instance = Utils.classForName[RecordWriter](klass).getConstructor().
        newInstance()
      instance.initialize(outputStream, conf)
      instance
    }
  }
}
