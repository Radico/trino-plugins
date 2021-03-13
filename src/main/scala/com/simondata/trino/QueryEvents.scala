/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._
import com.simondata.util.{Config, Time, Types, XRay}
import io.trino.spi.eventlistener.{EventListener, QueryCompletedEvent, QueryContext, QueryCreatedEvent, SplitCompletedEvent}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * The various query events that may be delivered to the EventListener.
 *
 * QueryStart - a query was submitted
 * QuerySplit - a split completed
 * QueryEnd - a query either succeeded or failure (errored, cancelled, timed-out)
 */
sealed trait QueryStage {
  def info: QueryInfo
}
case class QueryStart(info: QueryInfo, event: QueryCreatedEvent) extends QueryStage
case class QuerySplit(info: QueryInfo, stage: String, task: String, event: SplitCompletedEvent) extends QueryStage
case class QueryEnd(info: QueryInfo, event: QueryCompletedEvent) extends QueryStage
object QueryStage {
  def from(event: QueryCreatedEvent): QueryStart = QueryStart(QueryInfo.from(event), event)
  def from(event: SplitCompletedEvent): QuerySplit = QuerySplit(QueryInfo.from(event), event.getStageId, event.getTaskId, event)
  def from(event: QueryCompletedEvent): QueryEnd = QueryEnd(QueryInfo.from(event), event)
}

/**
 * Query failure summary.
 *
 * @param code a stable identifier for the failure (e.g., COLUMN_NOT_FOUND)
 * @param message
 * @param category a failure type (e.g., USER_ERROR, INTERNAL_ERROR)
 */
case class FailureInfo(
  code: String,
  message: Option[String],
  category: Option[String],
)

case class TimeInfo(
   created: Instant,
   started: Option[Instant] = None,
   ended: Option[Instant] = None,
) {
  def createdIso: String = Time.toIso(created)
  def startedIso: String = started.map(Time.toIso(_)).getOrElse("")
  def endedIso: String = ended.map(Time.toIso(_)).getOrElse("")
  def waitDuration: Duration = started.map(Duration.between(created, _)).getOrElse(Duration.ZERO)
  def runDuration: Duration = Types.zip(started, ended).map(d => Duration.between(d._1, d._2)).getOrElse(Duration.ZERO)
  def totalDuration: Duration = ended.map(Duration.between(created, _)).getOrElse(Duration.ZERO)
}

/**
 * Custom representation of a query's state, useful for matchin.
 *
 * @param id the query ID assigned by Trino
 * @param state the current state
 * @param time timing details (query creation, execution, completion as available)
 * @param resource the target schema (if applicable)
 * @param user the user who submitted the query
 * @param tags andy client-tags supplied when the query was submitted
 * @param failure failure details (if the event is associated with a failed split or query)
 * @param queryType the type of query as categorized by Trino (SELECT, INSERT, etc.)
 */
case class QueryInfo(
  id: String,
  state: String,
  time: TimeInfo,
  resource: Option[Resource] = None,
  user: Option[String] = None,
  tags: List[String] = Nil,
  failure: Option[FailureInfo] = None,
  queryType: Option[String] = None
) {
  def authId: AuthId = user.map(AuthIdUser(_)).getOrElse(AuthIdUnknown)
  def failed: Boolean = failure.isDefined
}

/**
 * Facilities to translate events from Trino's EventListener events into QueryInfo.
 */
object QueryInfo {
  def determineQueryType(context: QueryContext): Option[String] = {
    Types.toOption(context.getQueryType) map { _.name }
  }

  def deriveResource(context: QueryContext): Resource = (
    Types.toOption(context.getCatalog),
    Types.toOption(context.getSchema)
  ) match {
    case (Some(catalog), Some(schema)) => Schema(schema, Catalog(catalog))
    case (Some(catalog), None) => Catalog(catalog)
    case _ => UnknownResource
  }

  def from(event: QueryCreatedEvent): QueryInfo = {
    val created = event.getCreateTime
    val context = event.getContext
    val meta = event.getMetadata
    val tags = context.getClientTags.asScala.toList

    val user = context.getUser
    val resource = deriveResource(context)

    val id = meta.getQueryId
    val state = meta.getQueryState
    val queryType = determineQueryType(context)

    QueryInfo(
      id,
      state,
      time = TimeInfo(created),
      resource = Some(resource),
      user = Some(user),
      tags = tags,
      queryType = queryType
    )
  }

  def from(event: SplitCompletedEvent): QueryInfo = {
    val id = event.getQueryId
    val created = event.getCreateTime
    val started = Types.toOption(event.getStartTime)
    val ended = Types.toOption(event.getEndTime)
    val failure = Types.toOption(event.getFailureInfo) map { f =>
      FailureInfo(
        code = f.getFailureType,
        message = Some(f.getFailureMessage),
        category = None
      )
    }

    QueryInfo(
      id,
      state = "RUNNING",
      time = TimeInfo(created, started, ended),
      failure = failure
    )
  }

  def from(event: QueryCompletedEvent): QueryInfo = {
    val user = event.getContext.getUser
    val meta = event.getMetadata
    val context = event.getContext
    val id = meta.getQueryId
    val state = meta.getQueryState
    val created = event.getCreateTime
    val started = Some(event.getExecutionStartTime)
    val ended = Some(event.getEndTime)
    val resource = deriveResource(context)
    val queryType = determineQueryType(context)
    val tags = context.getClientTags.asScala.toList
    val failure = Types.toOption(event.getFailureInfo) map { f =>
      FailureInfo(
        code = f.getErrorCode.getName,
        message = Types.toOption(f.getFailureMessage),
        category = Some(f.getErrorCode.getType.name)
      )
    }

    QueryInfo(
      id,
      state,
      time = TimeInfo(created, started, ended),
      resource = Some(resource),
      user = Some(user),
      tags = tags,
      failure = failure,
      queryType = queryType
    )
  }
}

/**
 * Implement this trait and add to QueryEvents.instance() below to add custom query event handling.
 */
trait QueryEventsListener {
  def queryStart(stage: QueryStart)(implicit ec: ExecutionContext, pc: PluginContext): Future[Unit]
  def splitEnd(stage: QuerySplit)(implicit ec: ExecutionContext, pc: PluginContext): Future[Unit]
  def queryEnd(stage: QueryEnd)(implicit ec: ExecutionContext, pc: PluginContext): Future[Unit]
}

/**
 * The default QueryEventsListener implementation which logs query events to stdout and Slack.
 */
class QueryEventLogger extends QueryEventsListener {
  override def queryStart(stage: QueryStart)(
    implicit ec: ExecutionContext,
    pc: PluginContext
  ): Future[Unit] = Future {
    implicit val log = Logger.log(stage.info.authId)
    // Always log the query start event
    log.info(s"event-query-created => ${stage.info.id}")
    logQueryInfo(stage, slackOverride = Config.slackQueryCreated)
  }

  override def splitEnd(stage: QuerySplit)(
    implicit ec: ExecutionContext,
    pc: PluginContext
  ): Future[Unit] = Future {
    // Only log split events if configured to do so (disabled by default)
    Config.logSplitComplete match {
      case Some(true) => {
        implicit val log = Logger.log(stage.info.authId)
        log.info(s"event-split-completed => ${stage.info.id}")
        logQueryInfo(stage, slackOverride = Config.slackSplitComplete)
      }
      case _ =>
    }
  }

  override def queryEnd(stage: QueryEnd)(
    implicit ec: ExecutionContext,
    pc: PluginContext
  ): Future[Unit] = Future {
    implicit val log = Logger.log(stage.info.authId)
    // Always log query completion
    log.info(s"event-query-completed => ${stage.info.id}")
    logQueryInfo(stage)
  }

  private def queryPrefix(info: QueryInfo): String = {
    val id = info.id
    val state = info.state
    val typeInfo = info.queryType.map(qt => s"$qt ").getOrElse("")
    val tagInfo = info.tags match {
      case Nil => ""
      case tagList => s" [${tagList.mkString(", ")}]"
    }
    s"${typeInfo}Query `${id}` _${state}_$tagInfo"
  }

  private def logQueryInfo(
    queryStage: QueryStage,
    slackOverride: Option[Boolean] = None
  )(implicit log: Logger): Unit = Try {
    val (
      logLevel: LogLevel,
      logMessage: Option[String],
      slackOverride: Option[Boolean],
      slackColor: Option[String],
      slackEmoji: Option[String]
    ) = queryStage match {
      case QueryStart(QueryInfo(_, _, time, Some(resource), Some(user), _, _, _), _) => {
        val prefix = queryPrefix(queryStage.info)
        val logMessage: Option[String] = Config.logQueryCreated match {
          case Some(false) => None
          case None | Some(true) => Some(
            s"""${prefix}
               |submitted by `${user}` against schema `${resource}`
               |created at _*${time.createdIso}*_""".stripMargin
          )
        }

        (InfoLevel, logMessage, Config.slackQueryCreated, None, None)
      }
      case QuerySplit(QueryInfo(_, _, time, _, _, _, None, _), stage, task, _) => {
        val prefix = queryPrefix(queryStage.info)
        val elapsed = Time.human(time.runDuration)
        val logMessage: Option[String] = Some(
          s"""${prefix}
             |completed split ${stage}.${task} (lasted _*${elapsed}*_)""".stripMargin
        )

        (InfoLevel, logMessage, Config.slackSplitComplete, None, None)
      }
      case QuerySplit(QueryInfo(_, _, time, _, _, _, Some(FailureInfo(code, message, category)), _), stage, task, _) => {
        val prefix = queryPrefix(queryStage.info)
        val elapsed = Time.human(time.runDuration)
        val msg = message.getOrElse("")
        val cat = category.getOrElse("UNCATEGORIZED")
        val failureMessage = s"""${cat}:${code} => ${msg}"""
        val logMessage: Option[String] = Some(
          s"""${prefix}
             |failed split ${stage}.${task} (lasted _*${elapsed}*_)
             |--
             |${failureMessage}""".stripMargin
        )

        (InfoLevel, logMessage, Config.slackSplitComplete, None, None)
      }
      case QueryEnd(QueryInfo(_, _, time, Some(resource), Some(user), _, None, _), _) => {
        val prefix = queryPrefix(queryStage.info)
        val elapsed = Time.human(time.totalDuration)
        val logMessage: Option[String] = Config.logQuerySuccess match {
          case Some(false) => None
          case None | Some(true) => Some(
            s"""${prefix}
               |submitted by `${user}` against schema `${resource}`
               |ended at _*${time.endedIso}*_ (lasted _*${elapsed}*_)
               |""".stripMargin
          )
        }

        (InfoLevel, logMessage, Config.slackQuerySuccess, None, None)
      }
      case QueryEnd(QueryInfo(_, _, time, Some(resource), Some(user), _, Some(FailureInfo(code, message, category)), _), _) => {
        val prefix = queryPrefix(queryStage.info)
        val elapsed = Time.human(time.totalDuration)
        val msg = message.getOrElse("")
        val cat = category.getOrElse("UNCATEGORIZED")
        val logMessage: Option[String] = Config.logQueryFailure match {
          case Some(false) => None
          case None | Some(true) => Some(
            s"""${prefix}
               |submitted by `${user}` against schema `${resource}`
               |ended at _*${time.endedIso}*_ (lasted _*${elapsed}*_)
               |--
               |*${cat}:${code}*
               |${msg}""".stripMargin
          )
        }

        (WarnLevel, logMessage, Config.slackQueryFailure, None, None)
      }
      case queryStage => (WarnLevel, Some(s"""Unrecognized query stage: ${queryStage}"""), None, None, None)
    }

    // Log query info if a message was generated
    logMessage foreach { msg => log.log(
      logLevel,
      _ => msg,
      sendToSlack = slackOverride,
      slackMessageColor = slackColor,
      slackLevelEmoji = slackEmoji
    ) }
  } match {
    case Success(_) =>
    case Failure(error) => {
      error.printStackTrace()
      log.error(s"Error logging info for query ${queryStage} : ${error}")
    }
  }
}

/**
 * The custom EventListener. The initial implementation just translates events into rich log messages.
 * By default, query start events bypass Slack and split events are completely ignored. These behaviors
 * are configurable via environment variables.
 */
class QueryEvents(listeners: List[QueryEventsListener]) extends EventListener {
  private implicit val pc: PluginContext = EventsPlugin
  private implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  /**
   * A wrapper common to all event listener handlers which will catch and log errors,
   * but permit queries to continue.
   *
   * @param action the event handler logic which will close over the event on method declaration
   */
  private def wrappedEventHandler(action: => Future[Unit]) = Try {
    action
  } match {
    case Success(_) =>
    case Failure(e) => {
      Try {
        println(s"Error in an event listener handler ${XRay.getCallerName()}!")
        e.printStackTrace()

        Logger.log.error(s"Error in the event listener. The stack trace is in the Coordinator's logs")
      } match {
        case Success(_) =>
        case Failure(e) => {
          println("Another error occurred while handling an error in the event listener.")
          e.printStackTrace()
        }
      }
    }
  }

  override def queryCreated(event: QueryCreatedEvent): Unit = listeners map { listener =>
    wrappedEventHandler {
      listener.queryStart(QueryStage.from(event))
    }
  }

  override def splitCompleted(event: SplitCompletedEvent): Unit = listeners map { listener =>
    wrappedEventHandler {
      listener.splitEnd(QueryStage.from(event))
    }
  }

  override def queryCompleted(event: QueryCompletedEvent): Unit = listeners map { listener =>
    wrappedEventHandler {
      listener.queryEnd(QueryStage.from(event))
    }
  }
}

object QueryEvents {
  val loggingListener = new QueryEventLogger

  // NOTE: Add your custom QueryEventsListener implementations to this list.
  val listeners = loggingListener :: Nil

  val instance: EventListener = new QueryEvents(listeners)
}
