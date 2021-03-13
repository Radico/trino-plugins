/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

import com.simondata.util.Types
import io.trino.spi.connector.{CatalogSchemaName, CatalogSchemaTableName}
import io.trino.spi.security.SystemSecurityContext

/**
 * The general concept of a namespace. This is used in multiple places and is context-specific.
 *
 * @param name a unique namespace identifier
 */
case class Namespace(name: String)

/**
 * Represents a resource which may be involved in an AuthQuery.
 */
trait Resource {
  def category: String
}

case class Session(property: Option[String], value: Option[String] = None) extends Resource {
  override def category: String = "session"
  override def toString: String = property
    .map(p => value match {
      case Some(v) => s"property:$p=$v"
      case None => s"property:$p"
    })
    .getOrElse("any-property")
}
object Session {
  def anyProp: Session = Session(None)
}

case class Catalog(name: String) extends Resource {
  override def category: String = "catalog"
  override def toString: String = name
}
object Catalog {
  def of(catalog: String): Catalog = Catalog(catalog)
  def of(schema: CatalogSchemaName): Catalog = Catalog(schema.getCatalogName)
  def of(table: CatalogSchemaTableName): Catalog = Catalog(table.getCatalogName)
}

case class Schema(name: String, catalog: Catalog) extends Resource {
  override def category: String = "schema"
  override def toString: String = s"${catalog}.${name}"
}
object Schema {
  def of(schema: CatalogSchemaName): Schema = Schema(schema.getSchemaName, Catalog.of(schema))
  def of(table: CatalogSchemaTableName): Schema = Schema(table.getSchemaTableName.getSchemaName, Catalog.of(table))
}

case class Table(name: String, schema: Schema) extends Resource {
  override def category: String = "table"
  override def toString: String = s"${schema}.${name}"
}
object Table {
  def of(table: CatalogSchemaTableName): Table = Table(table.getSchemaTableName.getTableName, Schema.of(table))
}

case class Column(name: String, table: Table) extends Resource {
  override def category: String = "column"
  override def toString: String = s"${table}.${name}"
}
object Column {
  def of(name: String, table: CatalogSchemaTableName): Column = Column(name, Table.of(table))
}

case class Record(table: Table) extends Resource {
  override def category: String = "record"
  override def toString: String = s"${table}"
}
object Record {
  def in(table: CatalogSchemaTableName): Record = Record(Table.of(table))
  def in(table: Table): Record = Record(table)
}

case class XFunction(name: String) extends Resource {
  override def category: String = "function"
  override def toString: String = s"${name}"
}
object XFunction {
  def of(name: String): XFunction = XFunction(name)
}

case class XProcedure(name: String) extends Resource {
  override def category: String = "procedure"
  override def toString: String = s"${name}"
}
object XProcedure {
  def of(name: String): XProcedure = XProcedure(name)
}

/**
 * Identifies a query resource against which auth permissions for a given
 * user may be evaluated. The presence of the id and/or the owner determines
 * the scope of the access being evaluated:
 * - (Some(query), Some(user)) : a specific query owned by a specific user
 * - (Some(query), None) : a specific query with an unknown owner
 * - (None, Some(user)) : any query owned by a specific user
 * - (None, None) : any query owned by any user
 *
 * @param id the ID of the query (if applicable)
 * @param owner the ID of the query owner (if applicable)
 */
case class XQuery(id: Option[String], owner: Option[AuthId]) extends Resource {
  override def category: String = "query"
  override def toString: String = s"${id.getOrElse("any_query")}@${owner.map(_.toString).getOrElse("any_user")}"
}
object XQuery {
  def any: XQuery = of(None, None)
  def of(id: Option[String], owner: Option[AuthId] = None): XQuery = XQuery(id, owner)
  def from(context: SystemSecurityContext): XQuery = of(Types.toOption(context.getQueryId).map(_.getId))
  def from(owner: String): XQuery = of(None, Some(AuthIdUser(owner)))
  def from(context: SystemSecurityContext, owner: String): XQuery = of(
    Types.toOption(context.getQueryId.map(_.getId)),
    Some(AuthIdUser(owner))
  )
}

case object SystemInfo extends Resource {
  override def category: String = "system-info"
  override def toString: String = "any"
}

case object UnknownResource extends Resource {
  override def category: String = "unknown"
  override def toString: String = "unknown-resource"
}

/**
 * Context to help identify the cluster in logs/notifications.
 *
 * @param name a name which uniquely identifies the cluster within your infrastructure
 */
sealed abstract class ClusterContext(val name: String)
case object UnknownCluster extends ClusterContext("UNKNOWN")
case class NamedCluster(clusterName: String) extends ClusterContext(clusterName)

/**
 * Context identifying the plugin from which a log/notification message originated.
 *
 * @param name the name of the plugin
 */
sealed abstract class PluginContext(val name: String)
case object UnknownPlugin extends PluginContext("trino-plugins")
case object LocalPlugin extends PluginContext("trino-local")
case object AuthPlugin extends PluginContext("trino-auth")
case object EventsPlugin extends PluginContext("trino-events")
object PluginContext {
  def forName(pluginName: String): PluginContext = pluginName match {
    case AuthPlugin.name => AuthPlugin
    case EventsPlugin.name => EventsPlugin
    case _ => UnknownPlugin
  }
}
