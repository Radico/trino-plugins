/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

import java.security.Principal
import java.util
import java.util.Optional
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import com.simondata.util.{Config, Types, XRay}
import io.trino.spi.QueryId
import io.trino.spi.`type`.Type
import io.trino.spi.connector.{CatalogSchemaName, CatalogSchemaRoutineName, CatalogSchemaTableName, ColumnMetadata, EntityKindAndName, EntityPrivilege, SchemaTableName}
import io.trino.spi.eventlistener.EventListener
import io.trino.spi.function.{FunctionKind, SchemaFunctionName}
import io.trino.spi.security.AccessDeniedException.{denyAddColumn, denyAlterColumn, denyCatalogAccess, denyCommentColumn, denyCommentTable, denyCommentView, denyCreateCatalog, denyCreateFunction, denyCreateMaterializedView, denyCreateSchema, denyCreateTable, denyCreateView, denyCreateViewWithSelect, denyDeleteTable, denyDenyEntityPrivilege, denyDenyTablePrivilege, denyDropCatalog, denyDropColumn, denyDropFunction, denyDropMaterializedView, denyDropSchema, denyDropTable, denyDropView, denyExecuteFunction, denyExecuteProcedure, denyExecuteQuery, denyExecuteTableProcedure, denyGrantEntityPrivilege, denyGrantSchemaPrivilege, denyGrantTablePrivilege, denyImpersonateUser, denyInsertTable, denyKillQuery, denyReadSystemInformationAccess, denyRefreshMaterializedView, denyRenameColumn, denyRenameMaterializedView, denyRenameSchema, denyRenameTable, denyRenameView, denyRevokeEntityPrivilege, denyRevokeSchemaPrivilege, denyRevokeTablePrivilege, denySelectColumns, denySetCatalogSessionProperty, denySetMaterializedViewProperties, denySetSchemaAuthorization, denySetSystemSessionProperty, denySetTableAuthorization, denySetTableProperties, denySetUser, denySetViewAuthorization, denyShowColumns, denyShowCreateSchema, denyShowCreateTable, denyShowFunctions, denyShowRoles, denyShowSchemas, denyShowTables, denyTruncateTable, denyUpdateTableColumns, denyViewQuery, denyWriteSystemInformationAccess}
import io.trino.spi.security.{Identity, Privilege, SystemAccessControl, SystemSecurityContext, TrinoPrincipal, ViewExpression}

/**
 * Custom metadata associated with a authorization check.
 */
trait AuthCheckMeta {
  def summary: String
}

/**
 * Provides a mechanism for identifying which SystemAccessControl method
 * originated a specific AuthQuery.
 */
case class CallerAuthCheckMeta(caller: String) extends AuthCheckMeta {
  override def summary: String = s"caller:${caller}"
}

/**
 * This class is responsible for mapping SystemAccessControl to an implementation of TrinoAuth.
 *
 * This is initialized with your custom TrinoAuth implementation in the TrinoPlugins Java class.
 *
 * @param auth your selected/custom TrinoAuth implementation
 */
class CustomSystemAccessControl(auth: TrinoAuth) extends SystemAccessControl {
  private implicit val pc: PluginContext = AuthPlugin

  /**
   * If false, auth checks are not enforced, but the outcomes of all
   * auth queries will still be logged.
   */
  private def enforceAuth = Config.enforceAuth

  private def onDeny(authResult: AuthResult)(action: => Unit): Unit = {
    (enforceAuth, authResult) match {
      case (true, AuthDenied(_, _)) => action
      case _ =>
    }
  }

  private def onAllow(authResult: AuthResult)(action: => Unit): Unit = {
    (enforceAuth, authResult) match {
      case (true, AuthAllowed(_)) => action
      case _ =>
    }
  }

  private def logAuthQuery(
     authResult: AuthResult,
     filtering: Boolean = false
  )(implicit meta: Option[AuthCheckMeta]): Unit = {
    val logger = Logger.log(authResult.query.id)

    val id = authResult.query.id
    val action = authResult.query.action
    val resource = authResult.query.resource
    val metaStr = meta.map(_.summary)getOrElse("")

    def lockEmoji(slack: Boolean): String = if (slack) ":lock: " else ""

    (authResult, filtering) match {
      case (AuthAllowed(_), true) => logger.info(s => s"${lockEmoji(s)}Auth-Query [filtering] ${metaStr} ${id} ${action} ${resource} => ALLOWED")
      case (AuthAllowed(_), false) => logger.info(s => s"${lockEmoji(s)}Auth-Query ${metaStr} ${id} ${action} ${resource} => ALLOWED")
      case (AuthDenied(_, reason), true) => logger.info(s => s"${lockEmoji(s)}Auth-Query [filtering] ${metaStr} ${id} ${action} ${resource} => DENIED : ${reason}")
      case (AuthDenied(_, reason), false) => logger.warn(s => s"${lockEmoji(s)}Auth-Query ${metaStr} ${id} ${action} ${resource} => DENIED : ${reason}")
    }
  }

  private def logFilterRequest(filterResult: FilterResult)(implicit meta: Option[AuthCheckMeta]): Unit = {
    val logger = Logger.log(filterResult.request.id)
    val totalCount = filterResult.request.authQueries.size
    val allowCount = filterResult.allowed.size

    // Log each of the filter results
    filterResult.allowed.foreach(logAuthQuery(_, true))
    filterResult.denied.foreach(logAuthQuery(_, true))

    logger.info(s"Filter-Request retained ${allowCount} of ${totalCount}")
  }

  private def logAuthFilter(filterResult: FilterResult)(implicit meta: Option[AuthCheckMeta]): Unit = {
    val logger = Logger.log(filterResult.request.id)
    val totalCount = filterResult.request.authQueries.size

    // Log each of the filter results
    filterResult.allowed.foreach(logAuthQuery(_))
    filterResult.denied.foreach(logAuthQuery(_))

    filterResult.denied.size match {
      case 0 => logger.info(s"Auth-Filter all passed filtering => ALLOWED")
      case denyCount => logger.warn(s"Auth-Filter ${denyCount} of ${totalCount} failed filtering => DENIED")
    }
  }

  private def logAction(): Unit = {
    val caller = XRay.getCallerName().get
    Logger.log.info(s"Auth-Action ${caller}")
  }

  /**
   * Convenience method to perform authorization check, log the outcome,
   *
   * @param authQuery the auth query to evaluate
   * @param denyAction the action to take if authorization is denied
   */
  private def evaluateAuthQuery(authQuery: AuthQuery)(denyAction: => Unit): Unit = {
    implicit val callingMethod: Option[AuthCheckMeta] = XRay.getCallerName() map { CallerAuthCheckMeta(_) }
    val authResult = auth.authorize(authQuery)
    logAuthQuery(authResult)
    onDeny(authResult) { denyAction }
  }

  /**
   * Filter resources based on those with passing auth queries.
   *
   * @param filterRequest the auth queries to filter
   *
   * @return a FilterResult with the queries partitioned by whether they are allowed or denied
   */
  private def evaluateFilterRequest(filterRequest: FilterRequest)(implicit meta: Option[AuthCheckMeta]): FilterResult = {
    val filterResult = auth.filter(filterRequest)
    logFilterRequest(filterResult)
    filterResult
  }

  /**
   * Filter a set of resources down to those permitted for a given identity + action.
   *
   * @param context the context from which the identity will be assembled
   * @param resources the resources to filter
   * @param resourceBuilder translates resources to the appropriate AuthResource type
   *
   * @return the set of resources which passed the filter
   */
  private def filterResources[T](
    context: SystemSecurityContext,
    resources: List[T],
    authAction: AuthAction = AuthActionRead
  )(
    resourceBuilder: T => AuthResource
  ): List[T] = {
    implicit val callingMethod: Option[AuthCheckMeta] = XRay.getCallerName() map { CallerAuthCheckMeta(_) }
    var resourceMap: Map[AuthResource, T] = Map.empty
    val id = AuthId.of(context)
    val authQueries = resources map { resource =>
      val authResource = resourceBuilder(resource)
      resourceMap += authResource -> resource
      AuthQuery(id, authAction, authResource)
    }

    val filterResult = evaluateFilterRequest(FilterRequest(id, authQueries))

    enforceAuth match {
      case false => resources
      case true => {
        filterResult.allowed
          .map(_ match {
            case AuthAllowed(AuthQuery(_, _, resource)) => resourceMap.get(resource)
            case _ => None
          })
          .filter(_.isDefined)
          .map(_.get)
      }
    }
  }

  /**
   * Filter a set of resources down to those permitted for a given identity + action.
   *
   * @param context         the context from which the identity will be assembled
   * @param resources       the resources to filter
   * @param resourceBuilder translates resources to the appropriate AuthResource type
   * @return the set of resources which passed the filter
   */
  private def filterIdentities[T](
                                   identity: Identity,
                                   resources: List[T],
                                   authAction: AuthAction = AuthActionRead
                                 )(
                                   resourceBuilder: T => AuthResource
                                 ): List[T] = {
    implicit val callingMethod: Option[AuthCheckMeta] = XRay.getCallerName() map {
      CallerAuthCheckMeta(_)
    }
    var resourceMap: Map[AuthResource, T] = Map.empty
    val id = AuthId.of(identity)
    val authQueries = resources map { resource =>
      val authResource = resourceBuilder(resource)
      resourceMap += authResource -> resource
      AuthQuery(id, authAction, authResource)
    }

    val filterResult = evaluateFilterRequest(FilterRequest(id, authQueries))

    enforceAuth match {
      case false => resources
      case true => {
        filterResult.allowed
          .map(_ match {
            case AuthAllowed(AuthQuery(_, _, resource)) => resourceMap.get(resource)
            case _ => None
          })
          .filter(_.isDefined)
          .map(_.get)
      }
    }
  }


  /**
   * Evaluate a filter request, but instead of returning a result containing the
   * filtered subset of passing resources, only pass if all resources pass filtering.
   *
   * @param filterRequest
   * @param denyAction
   */
  private def evaluateAuthFilter(filterRequest: FilterRequest)(denyAction: => Unit): Unit = {
    implicit val callingMethod: Option[AuthCheckMeta] = XRay.getCallerName() map { CallerAuthCheckMeta(_) }
    val filterResult = auth.filter(filterRequest)
    logAuthFilter(filterResult)

    if (enforceAuth && !filterResult.denied.isEmpty) {
      denyAction
    }
  }

  override def checkCanImpersonateUser(identity: Identity, userName: String): Unit = {
    val id: AuthId = AuthId.of(identity)
    evaluateAuthQuery(
      AuthQuery(id, AuthActionUpdate, AuthResourceSession(Session(Some("user"), Some(userName))))
    ) {
      denyImpersonateUser(id.name, userName)
    }
  }

  override def checkCanSetUser(principal: Optional[Principal], userName: String): Unit = {
    val id: AuthId = Types.toOption(principal).map(AuthId.of(_)).getOrElse(AuthIdUnknown)
    evaluateAuthQuery(
      AuthQuery(id, AuthActionUpdate, AuthResourceSession(Session(Some("user"), Some(userName))))
    ) {
      denySetUser(principal, userName)
    }
  }

  override def checkCanExecuteQuery(identity: Identity, queryId: QueryId): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionExecute, AuthResourceQuery(XQuery.from(queryId, identity)))
    ) {
      denyExecuteQuery()
    }
  }

  override def filterCatalogs(context: SystemSecurityContext, catalogs: util.Set[String]): util.Set[String] = {
    val allowed = filterResources[String](
      context,
      catalogs.asScala.toList
    ) { catalogName =>
      AuthResourceCatalog(Catalog(catalogName))
    } toSet

    allowed asJava
  }

  override def checkCanCreateSchema(context: SystemSecurityContext, schema: CatalogSchemaName, properties: util.Map[String, AnyRef]): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionCreate, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyCreateSchema(schema.toString)
    }
  }

  override def checkCanDropSchema(context: SystemSecurityContext, schema: CatalogSchemaName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyDropSchema(schema.toString)
    }
  }

  override def checkCanRenameSchema(context: SystemSecurityContext, schema: CatalogSchemaName, newSchemaName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyRenameSchema(schema.toString, newSchemaName)
    }
  }

  override def checkCanSetSchemaAuthorization(context: SystemSecurityContext, schema: CatalogSchemaName, principal: TrinoPrincipal): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceSchema(Schema.of(schema)))
    ) {
      denySetSchemaAuthorization(schema.getSchemaName, principal)
    }
  }

  override def checkCanShowSchemas(context: SystemSecurityContext, catalogName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceCatalog(Catalog.of(catalogName)))
    ) {
      denyShowSchemas()
    }
  }

  override def filterSchemas(context: SystemSecurityContext, catalogName: String, schemaNames: util.Set[String]): util.Set[String] = {
    val allowed = filterResources[String](
      context,
      schemaNames.asScala.toList
    ) { schemaName =>
      AuthResourceSchema(Schema(schemaName, Catalog(catalogName)))
    } toSet

    allowed asJava
  }

  override def checkCanShowCreateSchema(context: SystemSecurityContext, schemaName: CatalogSchemaName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceSchema(Schema.of(schemaName)))
    ) {
      denyShowCreateSchema(schemaName.getSchemaName)
    }
  }

  override def checkCanShowCreateTable(context: SystemSecurityContext, tableName: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceTable(Table.of(tableName)))
    ) {
      denyShowCreateTable(tableName.getSchemaTableName.getTableName)
    }
  }

  override def checkCanCreateTable(context: SystemSecurityContext, table: CatalogSchemaTableName, properties: java.util.Map[String, Object]): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionCreate, AuthResourceTable(Table.of(table)))
    ) {
      denyCreateTable(table.toString)
    }
  }

  override def checkCanDropTable(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceTable(Table.of(table)))
    ) {
      denyDropTable(table.toString)
    }
  }

  override def checkCanRenameTable(context: SystemSecurityContext, table: CatalogSchemaTableName, newTable: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyRenameTable(table.toString, newTable.toString)
    }
  }

  override def checkCanSetColumnComment(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyCommentColumn(table.getSchemaTableName.getTableName)
    }
  }

  override def checkCanShowTables(context: SystemSecurityContext, schema: CatalogSchemaName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyShowTables(schema.getSchemaName)
    }
  }

  override def filterTables(context: SystemSecurityContext, catalogName: String, tableNames: util.Set[SchemaTableName]): util.Set[SchemaTableName] = {
    val allowed = filterResources[SchemaTableName](
      context,
      tableNames.asScala.toList
    ) { table =>
      AuthResourceTable(Table(table.getTableName, Schema(table.getSchemaName, Catalog(catalogName))))
    } toSet

    allowed asJava
  }

  override def checkCanShowColumns(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceTable(Table.of(table)))
    ) {
      denyShowColumns(table.getSchemaTableName.getTableName)
    }
  }

  override def checkCanAddColumn(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyAddColumn(table.toString)
    }
  }

  override def checkCanDropColumn(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyDropColumn(table.toString)
    }
  }

  override def checkCanRenameColumn(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyRenameColumn(table.toString)
    }
  }

  override def checkCanSelectFromColumns(context: SystemSecurityContext, table: CatalogSchemaTableName, columns: util.Set[String]): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceTable(Table.of(table)))
    ) {
      denySelectColumns(table.toString, columns)
    }
  }

  override def checkCanInsertIntoTable(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionCreate, AuthResourceRecord(Record.in(table)))
    ) {
      denyInsertTable(table.toString)
    }
  }

  override def checkCanDeleteFromTable(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceRecord(Record.in(table)))
    ) {
      denyDeleteTable(table.toString)
    }
  }

  override def checkCanCreateView(context: SystemSecurityContext, view: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionCreate, AuthResourceView(Table.of(view)))
    ) {
      denyCreateView(view.toString)
    }
  }

  override def checkCanRenameView(context: SystemSecurityContext, view: CatalogSchemaTableName, newView: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceView(Table.of(view)))
    ) {
      denyRenameView(view.getSchemaTableName.getTableName, newView.getSchemaTableName.getTableName)
    }
  }

  override def checkCanDropView(context: SystemSecurityContext, view: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceView(Table.of(view)))
    ) {
      denyDropView(view.toString)
    }
  }

  override def checkCanCreateViewWithSelectFromColumns(context: SystemSecurityContext, cstn: CatalogSchemaTableName, columns: util.Set[String]): Unit = {
    val id = AuthId.of(context)
    val table = Table.of(cstn)

    val queries = columns.asScala
      .toList
      .map(column => {
        AuthQuery(id, AuthActionRead, AuthResourceColumn(Column(column, table)))
      }) ::: AuthQuery(id, AuthActionCreate, AuthResourceView(table)) :: Nil

    evaluateAuthFilter(
      FilterRequest(id, queries)
    ) {
      denyCreateViewWithSelect(table.toString, context.getIdentity)
    }
  }

  override def checkCanSetCatalogSessionProperty(context: SystemSecurityContext, catalogName: String, propertyName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceCatalog(Catalog(catalogName)))
    ) {
      denySetCatalogSessionProperty(propertyName)
    }
  }

  override def checkCanGrantTablePrivilege(context: SystemSecurityContext, privilege: Privilege, table: CatalogSchemaTableName, grantee: TrinoPrincipal, withGrantOption: Boolean): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionGrant, AuthResourceTable(Table.of(table)))
    ) {
      denyGrantTablePrivilege(privilege.toString, table.toString)
    }
  }

  override def checkCanRevokeTablePrivilege(context: SystemSecurityContext, privilege: Privilege, table: CatalogSchemaTableName, revokee: TrinoPrincipal, grantOptionFor: Boolean): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRevoke, AuthResourceTable(Table.of(table)))
    ) {
      denyRevokeTablePrivilege(privilege.toString, table.toString)
    }
  }

  override def checkCanShowRoles(context: SystemSecurityContext): Unit = {
      denyShowRoles()
  }

  override def checkCanExecuteProcedure(context: SystemSecurityContext, procedure: CatalogSchemaRoutineName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionExecute, AuthResourceProcedure(XProcedure.of(procedure.getRoutineName)))
    ) {
      denyExecuteProcedure(procedure.getRoutineName)
    }
  }

  override def checkCanSetTableAuthorization(context: SystemSecurityContext, table: CatalogSchemaTableName, principal: TrinoPrincipal): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denySetTableAuthorization(table.toString, principal)
    }
  }

  override def checkCanSetTableComment(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyCommentTable(table.toString)
    }
  }

  override def checkCanSetTableProperties(context: SystemSecurityContext, table: CatalogSchemaTableName, properties: util.Map[String, Optional[AnyRef]]): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denySetTableProperties(table.toString)
    }
  }

  /*override def filterColumns(context: SystemSecurityContext, table: CatalogSchemaTableName, columns: util.Set[String]): util.Set[String] = super.filterColumns(context, table, columns)*/

  override def checkCanAlterColumn(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyAlterColumn(table.toString)
    }
  }

  /*override def checkCanGrantExecuteFunctionPrivilege(context: SystemSecurityContext, functionKind: FunctionKind, functionName: CatalogSchemaRoutineName, grantee: TrinoPrincipal, grantOption: Boolean): Unit = super.checkCanGrantExecuteFunctionPrivilege(context, functionKind, functionName, grantee, grantOption)*/

  override def checkCanTruncateTable(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceTable(Table.of(table)))
    ) {
      denyTruncateTable(table.getSchemaTableName.getTableName)
    }
  }

  override def checkCanUpdateTableColumns(securityContext: SystemSecurityContext, table: CatalogSchemaTableName, updatedColumnNames: util.Set[String]): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(securityContext), AuthActionRead, AuthResourceTable(Table.of(table)))
    ) {
      denyUpdateTableColumns(table.toString, updatedColumnNames)
    }
  }

  override def checkCanSetViewAuthorization(context: SystemSecurityContext, view: CatalogSchemaTableName, principal: TrinoPrincipal): Unit = {
    // TODO: principal argument is unused, so doing basic validation against view and context
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionGrant, AuthResourceView(Table.of(view)))
    ) {
      denySetViewAuthorization(view.toString, principal)
    }
  }

  override def checkCanSetViewComment(context: SystemSecurityContext, view: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceView(Table.of(view)))
    ) {
      denyCommentView(view.toString)
    }
  }

  override def checkCanCreateMaterializedView(context: SystemSecurityContext, materializedView: CatalogSchemaTableName, properties: util.Map[String, AnyRef]): Unit = {
    // TODO: properties argument is unused, so doing basic validation against materializedView and context
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionCreate, AuthResourceView(Table.of(materializedView)))
    ) {
      denyCreateMaterializedView(materializedView.toString)
    }
  }

  override def checkCanRefreshMaterializedView(context: SystemSecurityContext, materializedView: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceView(Table.of(materializedView)))
    ) {
      denyRefreshMaterializedView(materializedView.toString)
    }
  }

  override def checkCanSetMaterializedViewProperties(context: SystemSecurityContext, materializedView: CatalogSchemaTableName, properties: util.Map[String, Optional[AnyRef]]): Unit = {
    // TODO: properties argument is unused, so doing basic validation against materializedView and context
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceView(Table.of(materializedView)))
    ) {
      denySetMaterializedViewProperties(materializedView.toString)
    }
  }

  override def checkCanDropMaterializedView(context: SystemSecurityContext, materializedView: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceView(Table.of(materializedView)))
    ) {
      denyDropMaterializedView(materializedView.toString)
    }
  }

  override def checkCanRenameMaterializedView(context: SystemSecurityContext, view: CatalogSchemaTableName, newView: CatalogSchemaTableName): Unit = {
    // Validate current view
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceView(Table.of(view)))
    ) {
      denyRenameMaterializedView(view.toString, newView.toString)
    }

    // Validate new view
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceView(Table.of(newView)))
    ) {
      denyRenameMaterializedView(view.toString, newView.toString)
    }
  }

  override def checkCanGrantSchemaPrivilege(context: SystemSecurityContext, privilege: Privilege, schema: CatalogSchemaName, grantee: TrinoPrincipal, grantOption: Boolean): Unit = {
    // TODO: Unused options for grantee, grantOption
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionGrant, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyGrantSchemaPrivilege(privilege.toString, schema.toString)
    }
  }


  override def checkCanDenySchemaPrivilege(context: SystemSecurityContext, privilege: Privilege, schema: CatalogSchemaName, grantee: TrinoPrincipal): Unit = {
    // TODO: Unused options for grantee, grantOption
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDeny, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyGrantSchemaPrivilege(privilege.toString, schema.toString)
    }
  }


  override def checkCanRevokeSchemaPrivilege(context: SystemSecurityContext, privilege: Privilege, schema: CatalogSchemaName, revokee: TrinoPrincipal, grantOption: Boolean): Unit = {
    // TODO: Unused options for revokee, grantOption
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRevoke, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyRevokeSchemaPrivilege(privilege.toString, schema.toString)
    }
  }


  override def checkCanDenyTablePrivilege(context: SystemSecurityContext, privilege: Privilege, table: CatalogSchemaTableName, grantee: TrinoPrincipal): Unit = {
    // TODO: Unused options for grantee, grantOption
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDeny, AuthResourceTable(Table.of(table)))
    ) {
      denyDenyTablePrivilege(privilege.toString, table.toString)
    }
  }

  override def checkCanExecuteTableProcedure(systemSecurityContext: SystemSecurityContext, table: CatalogSchemaTableName, procedure: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(systemSecurityContext), AuthActionExecute, AuthResourceProcedure(XProcedure.of(procedure)))
    ) {
      denyExecuteTableProcedure(table.toString, procedure)
    }
  }

  override def getRowFilters(context: SystemSecurityContext, tableName: CatalogSchemaTableName): util.List[ViewExpression] = {
    new java.util.ArrayList[ViewExpression]()
  }

  override def checkCanCreateCatalog(context: SystemSecurityContext, catalog: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionCreate, AuthResourceCatalog(Catalog(catalog)))
    ) {
      denyCreateCatalog(catalog)
    }
  }

  override def checkCanDropCatalog(context: SystemSecurityContext, catalog: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDelete, AuthResourceCatalog(Catalog(catalog)))
    ) {
      denyDropCatalog(catalog)
    }
  }


  override def checkCanViewQueryOwnedBy(identity: Identity, queryOwner: Identity): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionRead, AuthResourceIdentity(XIdentity.from(queryOwner)))
    ) {
      denyViewQuery(queryOwner.getUser)
    }
  }

  override def filterViewQueryOwnedBy(identity: Identity, queryOwners: util.Collection[Identity]): util.Collection[Identity] = {
    val allowed = filterIdentities[Identity](
      identity,
      queryOwners.asScala.toList
    ) { owner =>
      AuthResourceIdentity(XIdentity.from(owner))
    } toSet

    allowed asJava
  }

  override def checkCanKillQueryOwnedBy(identity: Identity, queryOwner: Identity): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionKill, AuthResourceIdentity(XIdentity.from(queryOwner)))
    ) {
      denyKillQuery(queryOwner.getUser)
    }
  }

  override def checkCanReadSystemInformation(identity: Identity): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionRead, AuthResourceSystemInfo)
    ) {
      denyReadSystemInformationAccess()
    }
  }

  override def checkCanWriteSystemInformation(identity: Identity): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionUpdate, AuthResourceSystemInfo)
    ) {
      denyWriteSystemInformationAccess()
    }
  }

  override def checkCanSetSystemSessionProperty(identity: Identity, propertyName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionUpdate, AuthResourceSession(Session(Some(propertyName))))
    ) {
      denySetSystemSessionProperty(propertyName)
    }
  }

  override def canAccessCatalog(context: SystemSecurityContext, catalogName: String): Boolean = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceCatalog(Catalog(catalogName)))
    ) {
      denyCatalogAccess(catalogName)
    }
    true
  }

  override def checkCanGrantEntityPrivilege(context: SystemSecurityContext, privilege: EntityPrivilege, entity: EntityKindAndName, grantee: TrinoPrincipal, grantOption: Boolean): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionGrant, AuthResourcePrivilege(XEntityPrivilege.of(privilege.name())))
    ) {
      denyGrantEntityPrivilege(privilege.name(), entity)
    }
  }

  override def checkCanDenyEntityPrivilege(context: SystemSecurityContext, privilege: EntityPrivilege, entity: EntityKindAndName, grantee: TrinoPrincipal): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionDeny, AuthResourcePrivilege(XEntityPrivilege.of(privilege.name())))
    ) {
      denyDenyEntityPrivilege(privilege.name(), entity)
    }
  }

  override def checkCanRevokeEntityPrivilege(context: SystemSecurityContext, privilege: EntityPrivilege, entity: EntityKindAndName, revokee: TrinoPrincipal, grantOption: Boolean): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRevoke, AuthResourcePrivilege(XEntityPrivilege.of(privilege.name())))
    ) {
      denyRevokeEntityPrivilege(privilege.name(), entity)
    }
  }


  override def canExecuteFunction(systemSecurityContext: SystemSecurityContext, functionName: CatalogSchemaRoutineName): Boolean = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(systemSecurityContext), AuthActionExecute, AuthResourceFunction(XFunction.of(functionName.getRoutineName)))
    ) {
      denyExecuteFunction(functionName.getRoutineName)
    }
    true
  }

  override def canCreateViewWithExecuteFunction(systemSecurityContext: SystemSecurityContext, functionName: CatalogSchemaRoutineName): Boolean = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(systemSecurityContext), AuthActionCreate, AuthResourceFunction(XFunction.of(functionName.getRoutineName)))
    ) {
      denyExecuteFunction(functionName.getRoutineName)
    }
    true
  }

  override def checkCanShowFunctions(context: SystemSecurityContext, schema: CatalogSchemaName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceSchema(Schema.of(schema)))
    ) {
      denyShowFunctions(schema.toString)
    }
  }

  override def filterColumns(context: SystemSecurityContext, catalogName: String, tableColumns: util.Map[SchemaTableName, util.Set[String]]): util.Map[SchemaTableName, util.Set[String]] = {
    // TODO: we don't address more granular access than catalog.
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceCatalog(Catalog(catalogName)))
    ) {
      denyCatalogAccess(catalogName)
    }
    tableColumns
  }

  override def filterFunctions(context: SystemSecurityContext, catalogName: String, functionNames: util.Set[SchemaFunctionName]): util.Set[SchemaFunctionName] = {
    // TODO: we don't address more granular access than catalog.
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceCatalog(Catalog(catalogName)))
    ) {
      denyCatalogAccess(catalogName)
    }
    functionNames
  }

  override def checkCanCreateFunction(systemSecurityContext: SystemSecurityContext, functionName: CatalogSchemaRoutineName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(systemSecurityContext), AuthActionCreate, AuthResourceFunction(XFunction.of(functionName.getRoutineName)))
    ) {
      denyCreateFunction(functionName.getRoutineName)
    }
  }

  override def checkCanDropFunction(systemSecurityContext: SystemSecurityContext, functionName: CatalogSchemaRoutineName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(systemSecurityContext), AuthActionDelete, AuthResourceFunction(XFunction.of(functionName.getRoutineName)))
    ) {
      denyDropFunction(functionName.getRoutineName)
    }
  }


  override def checkCanSetSystemSessionProperty(identity: Identity, queryId: QueryId, propertyName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(identity), AuthActionUpdate, AuthResourceSession(Session(Some(propertyName))))
    ) {
      denySetSystemSessionProperty(propertyName)
    }
  }

  override def shutdown(): Unit = super.shutdown()

  override def getColumnMask(context: SystemSecurityContext, tableName: CatalogSchemaTableName, columnName: String, `type`: Type): Optional[ViewExpression] = {
    Optional.empty()
  }

  // TODO: Deny Role modifications for now

  override def checkCanCreateRole(context: SystemSecurityContext, role: String, grantor: Optional[TrinoPrincipal]): Unit = super.checkCanCreateRole(context, role, grantor)

  override def checkCanDropRole(context: SystemSecurityContext, role: String): Unit = super.checkCanDropRole(context, role)

  override def checkCanGrantRoles(context: SystemSecurityContext, roles: util.Set[String], grantees: util.Set[TrinoPrincipal], adminOption: Boolean, grantor: Optional[TrinoPrincipal]): Unit = super.checkCanGrantRoles(context, roles, grantees, adminOption, grantor)

  override def checkCanRevokeRoles(context: SystemSecurityContext, roles: util.Set[String], grantees: util.Set[TrinoPrincipal], adminOption: Boolean, grantor: Optional[TrinoPrincipal]): Unit = super.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor)

  override def checkCanShowCurrentRoles(context: SystemSecurityContext): Unit = super.checkCanShowCurrentRoles(context)

  override def checkCanShowRoleGrants(context: SystemSecurityContext): Unit = super.checkCanShowRoleGrants(context)

  override def getEventListeners(): java.lang.Iterable[EventListener] = {
    val listeners: List[EventListener] = QueryEvents.instance :: Nil

    listeners asJava
  }
}
