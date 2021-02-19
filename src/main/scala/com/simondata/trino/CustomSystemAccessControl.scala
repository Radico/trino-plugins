package com.simondata.trino

import java.security.Principal
import java.util
import java.util.Optional

import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import com.simondata.util.{Config, Types, XRay}
import io.trino.spi.`type`.Type
import io.trino.spi.connector.{CatalogSchemaName, CatalogSchemaRoutineName, CatalogSchemaTableName, ColumnMetadata, SchemaTableName}
import io.trino.spi.eventlistener.EventListener
import io.trino.spi.security.AccessDeniedException.{denyAddColumn, denyCatalogAccess, denyCommentColumn, denyCommentTable, denyCreateSchema, denyCreateTable, denyCreateView, denyCreateViewWithSelect, denyDeleteTable, denyDropColumn, denyDropSchema, denyDropTable, denyDropView, denyExecuteFunction, denyExecuteProcedure, denyExecuteQuery, denyGrantExecuteFunctionPrivilege, denyGrantTablePrivilege, denyImpersonateUser, denyInsertTable, denyReadSystemInformationAccess, denyRenameColumn, denyRenameSchema, denyRenameTable, denyRenameView, denyRevokeTablePrivilege, denySelectColumns, denySetCatalogSessionProperty, denySetSchemaAuthorization, denySetSystemSessionProperty, denySetUser, denyShowColumns, denyShowCreateSchema, denyShowCreateTable, denyShowRoles, denyShowSchemas, denyShowTables, denyViewQuery, denyWriteSystemInformationAccess}
import io.trino.spi.security.{TrinoPrincipal, Privilege, SystemAccessControl, SystemSecurityContext, ViewExpression}

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

  override def checkCanImpersonateUser(context: SystemSecurityContext, userName: String): Unit = {
    val id: AuthId = AuthId.of(context)
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

  override def checkCanExecuteQuery(context: SystemSecurityContext): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionExecute, AuthResourceQuery(XQuery.from(context)))
    ) {
      denyExecuteQuery()
    }
  }

  override def checkCanViewQueryOwnedBy(context: SystemSecurityContext, queryOwner: String): Unit = {
    val id: AuthId = AuthId.of(context)

    evaluateAuthQuery(
      AuthQuery(id, AuthActionRead, AuthResourceQuery(XQuery.from(context, queryOwner)))
    ) {
      denyViewQuery(s"${id} cannot view queries owned by ${AuthIdUser(queryOwner)}")
    }
  }

  override def filterViewQueryOwnedBy(context: SystemSecurityContext, queryOwners: util.Set[String]): util.Set[String] = {
    val allowed = filterResources[String](
      context,
      queryOwners.asScala.toList
    ) { owner =>
      AuthResourceQuery(XQuery.from(context, owner))
    } toSet

    allowed asJava
  }

  override def checkCanKillQueryOwnedBy(context: SystemSecurityContext, queryOwner: String): Unit = {
    val id: AuthId = AuthId.of(context)

    evaluateAuthQuery(
      AuthQuery(id, AuthActionDelete, AuthResourceQuery(XQuery.from(context, queryOwner)))
    ) {
      denyViewQuery(s"${id} cannot kill queries owned by ${AuthIdUser(queryOwner)}")
    }
  }

  override def checkCanReadSystemInformation(context: SystemSecurityContext): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceSystemInfo)
    ) {
      denyReadSystemInformationAccess()
    }
  }

  override def checkCanWriteSystemInformation(context: SystemSecurityContext): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceSystemInfo)
    ) {
      denyWriteSystemInformationAccess()
    }
  }

  override def checkCanSetSystemSessionProperty(context: SystemSecurityContext, propertyName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceSession(Session(Some(propertyName))))
    ) {
      denySetSystemSessionProperty(propertyName)
    }
  }

  override def checkCanAccessCatalog(context: SystemSecurityContext, catalogName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceCatalog(Catalog(catalogName)))
    ) {
      denyCatalogAccess(catalogName)
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

  override def checkCanCreateSchema(context: SystemSecurityContext, schema: CatalogSchemaName): Unit = {
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

  override def checkCanCreateTable(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
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

  override def checkCanSetTableComment(context: SystemSecurityContext, table: CatalogSchemaTableName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionUpdate, AuthResourceTable(Table.of(table)))
    ) {
      denyCommentTable(table.toString)
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

  /*
  override def filterColumns(context: SystemSecurityContext, table: CatalogSchemaTableName, columns: util.List[ColumnMetadata]): util.List[ColumnMetadata] = {
    val allowed = filterResources[ColumnMetadata](
      context,
      columns.asScala.toList
    ) { column =>
      AuthResourceColumn(Column(column.getName, Table.of(table)))
    }

    allowed asJava
  }
  */

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

  override def checkCanGrantExecuteFunctionPrivilege(context: SystemSecurityContext, functionName: String, grantee: TrinoPrincipal, grantOption: Boolean): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionGrant, AuthResourceFunction(XFunction.of(functionName)))
    ) {
      denyGrantExecuteFunctionPrivilege(functionName, context.getIdentity, grantee.getName)
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

  override def checkCanShowRoles(context: SystemSecurityContext, catalogName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionRead, AuthResourceCatalog(Catalog(catalogName)))
    ) {
      denyShowRoles(catalogName)
    }
  }

  override def checkCanExecuteProcedure(context: SystemSecurityContext, procedure: CatalogSchemaRoutineName): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionExecute, AuthResourceProcedure(XProcedure.of(procedure.getRoutineName)))
    ) {
      denyExecuteProcedure(procedure.getRoutineName)
    }
  }

  override def checkCanExecuteFunction(context: SystemSecurityContext, functionName: String): Unit = {
    evaluateAuthQuery(
      AuthQuery(AuthId.of(context), AuthActionExecute, AuthResourceFunction(XFunction.of(functionName)))
    ) {
      denyExecuteFunction(functionName)
    }
  }

  override def getRowFilter(context: SystemSecurityContext, tableName: CatalogSchemaTableName): Optional[ViewExpression] = {
    /*
    val expression: Option[ViewExpression] = Some(new ViewExpression(
      context.getIdentity.getUser,
      Types.toOptional(Some(tableName.getCatalogName)),
      Types.toOptional(Some(tableName.getSchemaTableName.getSchemaName)),
      "*"
    ))

    Types.toOptional(expression)
    */

    Optional.empty()
  }

  override def getColumnMask(context: SystemSecurityContext, tableName: CatalogSchemaTableName, columnName: String, `type`: Type): Optional[ViewExpression] = {
    /*
    val expression: Option[ViewExpression] = Some(new ViewExpression(
      context.getIdentity.getUser,
      Types.toOptional(Some(tableName.getCatalogName)),
      Types.toOptional(Some(tableName.getSchemaTableName.getSchemaName)),
      "*"
    ))

    Types.toOptional(expression)
    */

    Optional.empty()
  }

  override def getEventListeners(): java.lang.Iterable[EventListener] = {
    val listeners: List[EventListener] = QueryEvents.instance :: Nil

    listeners asJava
  }
}
