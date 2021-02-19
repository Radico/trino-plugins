package com.simondata.util

import com.simondata.trino.{ClusterContext, NamedCluster, PluginContext, UnknownCluster, UnknownPlugin}
import play.api.libs.json.{JsBoolean, JsNumber, JsObject, JsString, Json}

import scala.util.{Failure, Success, Try}

sealed abstract class Environment(val name: String)
case object ProdEnvironment extends Environment("prod")
case object DevEnvironment extends Environment("dev")

sealed abstract class NodeType(val name: String) {
  def isCoordinator: Boolean = false
}
case object CoordinatorNode extends NodeType("coordinator") {
  override def isCoordinator: Boolean = true
}
case object WorkerNode extends NodeType("worker")
case object UnknownNode extends NodeType("unknown")

trait ConfigSupplier {
  def get(key: String): Option[String]

  /**
   * Fetch and convert a config value.
   *
   * @param key the environment variable to fetch
   * @param convert function to convert form a string to the desired type
   *
   * @return None if conversion fails, otherwise Some(value)
   */
  def getAs[T](key: String)(convert: String => Option[T]): Option[T] = {
    Try {
      get(key) flatMap (convert(_))
    } match {
      case Failure(_) => None
      case Success(v) => v
    }
  }

  def getInt(key: String): Option[Int] = getAs(key)(_.toIntOption)
  def getToggle(key: String): Option[Boolean] = getAs(key)(ConfigSupplier.parseToggle(_))
  def getString(key: String): Option[String] = getAs(key) { _.trim match {
    case "" => None
    case s => Some(s)
  } }
}

object ConfigSupplier {
  private class MapConfigSupplier(map: Map[String, String]) extends ConfigSupplier {
    override def get(key: String): Option[String] = map.get(key)
  }

  private class ChainConfigSupplier(chain: List[ConfigSupplier]) extends ConfigSupplier {
    override def get(key: String): Option[String] = chain.view.map(_.get(key)).collectFirst { case Some(v) => v }
  }

  def of(map: Map[String, String]): ConfigSupplier = new MapConfigSupplier(map)
  def chain(chain: List[ConfigSupplier]): ConfigSupplier = new ChainConfigSupplier(chain)

  def parseToggle(value: String): Option[Boolean] = {
    value.toLowerCase match {
      case "true"  | "t" => Some(true)
      case "false" | "f" => Some(false)
      case "yes"   | "y" => Some(true)
      case "no"    | "n" => Some(false)
      case "on"    | "1" => Some(true)
      case "off"   | "0" => Some(false)
      case _ => None
    }
  }
}

object Config {
  val env = Env
  val props = Props.read("/etc/trino/cluster-info.properties")

  val logQueryCreated: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_LOG_QUERY_CREATED")
      .orElse(props.getToggle("events.log.query_created"))
  }

  val logQuerySuccess: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_LOG_QUERY_SUCCESS")
      .orElse(props.getToggle("events.log.query_success"))
  }

  val logQueryFailure: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_LOG_QUERY_FAILURE")
      .orElse(props.getToggle("events.log.query_failure"))
  }

  val logSplitComplete: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_LOG_SPLIT_COMPLETE")
      .orElse(props.getToggle("events.log.split_complete"))
  }

  val slackQueryCreated: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_SLACK_QUERY_CREATED")
      .orElse(props.getToggle("events.slack.query_created"))
  }

  val slackQuerySuccess: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_SLACK_QUERY_SUCCESS")
      .orElse(props.getToggle("events.slack.query_success"))
  }

  val slackQueryFailure: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_SLACK_QUERY_FAILURE")
      .orElse(props.getToggle("events.slack.query_failure"))
  }

  val slackSplitComplete: Option[Boolean] = {
    env
      .getToggle("TRINO_PLUGINS_SLACK_SPLIT_COMPLETE")
      .orElse(props.getToggle("events.slack.split_complete"))
  }

  def enforceAuth: Boolean = env
    .getToggle("TRINO_PLUGINS_ENFORCE_AUTH")
    .orElse(props.getToggle("auth.enforce"))
    .getOrElse(true)

  val environment: Environment = {
    env
      .getString("TRINO_PLUGINS_ENVIRONMENT")
      .orElse(props.getString("environment"))
      .getOrElse(ProdEnvironment.name) match {
        case DevEnvironment.name => DevEnvironment
        case ProdEnvironment.name => ProdEnvironment
      }
  }

  val slackWebhook: Option[String] = env
    .getString("TRINO_PLUGINS_SLACK_WEBHOOK_URL")
    .orElse(props.getString("notifications.slack.webhook"))

  val mdUrl = "http://169.254.169.254/latest"

  val instanceId: Option[String] = Try(Http.get(s"${mdUrl}/meta-data/instance-id"))
    .toOption
    .map(_.body)

  val userData: Option[JsObject] = Try(Http.get(s"${mdUrl}/user-data"))
    .toOption
    .map(_.body)
    .flatMap(txt => Try(Json.parse(txt)).toOption) match {
      case Some(obj: JsObject) => Some(obj)
      case _ => None
    }

  val cfConfig: Option[JsObject] = userData
    .map(_("cft_configure")) match {
      case Some(obj: JsObject) => Some(obj)
      case _ => None
    }

  val coordinatorIp: Option[String] = cfConfig
    .flatMap(_("coordinator_address") match {
      case JsString(address) => Some(address)
      case _ => None
    })

  val coordinatorPort: Option[Int] = cfConfig
    .flatMap(_("trino_http_port") match {
      case JsNumber(port) => Some(port.intValue)
      case _ => None
    })

  val cluster: ClusterContext = env
    .getString("TRINO_PLUGINS_CLUSTER")
    .orElse(props.getString("trino.cluster.name"))
    .orElse(cfConfig.map(_("cf_stack")) match {
      case Some(JsString(clusterName)) => Some(clusterName)
      case _ => None
    }) match {
      case Some(clusterName) => NamedCluster(clusterName)
      case _ => UnknownCluster
    }

  val nodeType: NodeType = env
    .getString("TRINO_PLUGINS_NODE_TYPE")
    .orElse(props.getString("trino.node.type"))
    .orElse(cfConfig.map(c => (c("coordinator"), c("worker"))) match {
      case Some((JsBoolean(true), JsBoolean(false))) => Some("coordinator")
      case Some((JsBoolean(false), JsBoolean(true))) => Some("worker")
      case _ => None
    }) match {
      case Some(CoordinatorNode.name) => CoordinatorNode
      case Some(WorkerNode.name) => WorkerNode
      case _ => UnknownNode
    }
}
