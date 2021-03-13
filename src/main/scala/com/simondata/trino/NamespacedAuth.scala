/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

/**
 * Example implementation of TrinoAuth which uses a simple schema naming convention
 * to validate whether a user is permitted work with schemas prefixed with `<namespace>_`.
 *
 * The example goes beyond just enforcing permissions based on the namespace. There are also
 * exmaples of shared schemas, read-only access to metadata tables, controls on which session
 * properties users may set, additional access permitted for the `root` and `admin` users.
 *
 * WARNING: This is meant to be a starting point for your own custom auth. While you can
 *          use this implementation as is, there are not guarantees regarding its
 *          compatibility with the needs of your cluster and clients.
 *
 * @param namespace the namespace prefix for schemas to which access should be moderated
 */
class NamespacedAuth(val namespace: String) extends TrinoAuth {
  def messages = TrinoAuth.messages

  val sharedSchemas = Set(
    "trino_shared",
    "trino_shared_dev"
  )
  def isSharedSchema(c: Column): Boolean = isSharedSchema(c.table)
  def isSharedSchema(t: Table): Boolean = isSharedSchema(t.schema)
  def isSharedSchema(s: Schema): Boolean = sharedSchemas contains s.name

  val permittedSchemaTables = Set(
    "columns",
    "tables",
    "views"
  )
  def canReadInformationSchemaTable(tableName: String): Boolean = permittedSchemaTables contains tableName

  val mutableSessionProps = Set(
    "query_max_execution_time",
    "query_max_stage_count",
    "spill_enabled",
    "spill_order_by",
    "spill_window_operator"
  )
  def canSetSessionProperty(sessionProperty: String): Boolean = mutableSessionProps contains sessionProperty

  override def authorize(authQuery: AuthQuery): AuthResult = {
    // Keep in mind that the match order is critical.
    // Matches further up in the sequence enforce denials/allowances
    // that are assumed further down the line.

    authQuery match {
      /**
       * User "root" can do anything.
       */
      case AuthQuery(
        AuthIdPrincipal("root") | AuthIdUser("root"),
        _,
        _
      ) => authQuery.allow()

      // Allow setting of the user as a non-user
      case AuthQuery(
        AuthIdUnknown | AuthIdPrincipal(_),
        AuthActionUpdate,
        AuthResourceSession(Session(Some("user"), _))
      ) => authQuery.allow()

      // Allow a user to re-set their own identity (oddly, this happens)
      case AuthQuery(
        AuthIdUser(user),
        AuthActionUpdate,
        AuthResourceSession(Session(Some("user"), Some(newUser)))
      ) if user == newUser => authQuery.allow()

      // Allow manual adjustment of permitted session properties
      case AuthQuery(
        AuthIdUser(_) | AuthIdPrincipal(_),
        AuthActionUpdate,
        AuthResourceSession(Session(Some(property), _))
      ) if canSetSessionProperty(property) => authQuery.allow()

      // Deny any action when the user is not identified
      case AuthQuery(AuthIdUnknown, _, _) =>
        authQuery.deny(messages.denyAnonymous)

      // Allow admin to set any session property (permits assuming identity)
      case AuthQuery(
        AuthIdPrincipal("admin") | AuthIdUser("admin"),
        AuthActionUpdate,
        AuthResourceSession(_)
      ) => authQuery.allow()

      // Allow admin to read system info
      case AuthQuery(
        AuthIdUser("admin"),
        AuthActionRead,
        AuthResourceSystemInfo
      ) => authQuery.allow()

      // Only allow root to cancel its own queries
      case AuthQuery(
        AuthIdUser(user),
        AuthActionDelete,
        AuthResourceQuery(XQuery(_, Some(AuthIdUser("root"))))
      ) if user != "root" => authQuery.deny(messages.denyDefault)

      // Deny all other access to system info
      case AuthQuery(
        _,
        _,
        AuthResourceSystemInfo
      ) => authQuery.deny(messages.denyUpdateSystemInfo)

      /**
       * Anything which is denied for admins must be specified above this point.
       * Currently these are:
       * - "root" -> can do anything
       * - "admin" -> can do most things
       */

      // Allow all other actions for admin
      case AuthQuery(
        AuthIdUser("admin"),
        _,
        _
      ) => authQuery.allow()

      // Deny any action outside of the hive catalog
      case AuthQuery(_, _, AuthResourceCatalog(Catalog(catalog))) if (catalog != "hive") =>
        authQuery.deny(messages.denyCatalog)

      // Allow read operations on the catalog
      case AuthQuery(
        AuthIdUser(_),
        AuthActionRead,
        AuthResourceCatalog(_)
      ) => authQuery.allow()

      // Allow read operations against certain hive.information_schema.
      // Trino will further validate access to requested sub-resources via subsequent auth filters.
      case AuthQuery(
        AuthIdUser(_),
        AuthActionRead,
        AuthResourceTable(Table(table, Schema("information_schema", Catalog("hive"))))
      ) if canReadInformationSchemaTable(table) => authQuery.allow()

      // Allow reading of metadata from the shared schemas
      case AuthQuery(
        AuthIdUser(_),
        AuthActionRead,
        AuthResourceSchema(schema)
      ) if isSharedSchema(schema) => authQuery.allow()

      // Allow CRUD operations against temporary tables in the shared schemas
      case AuthQuery(
        AuthIdUser(_),
        AuthActionCreate | AuthActionRead | AuthActionUpdate | AuthActionDelete,
        AuthResourceTable(table)
      ) if isSharedSchema(table) => authQuery.allow()
      case AuthQuery(
        AuthIdUser(_),
        AuthActionCreate | AuthActionRead | AuthActionUpdate | AuthActionDelete,
        AuthResourceColumn(column)
      ) if isSharedSchema(column) => authQuery.allow()

      // Allow read access to resources in the user's `<namespace>_<user>` schema
      case AuthQuery(
        AuthIdUser(user),
        AuthActionRead,
        AuthResourceSchema(Schema(schema, _))
      ) if (schema == s"${namespace}_${user}") => authQuery.allow()
      case AuthQuery(
        AuthIdUser(user),
        AuthActionRead,
        AuthResourceTable(Table(_, Schema(schema, _)))
      ) if (schema == s"${namespace}_${user}") => authQuery.allow()
      case AuthQuery(
        AuthIdUser(user),
        AuthActionRead,
        AuthResourceColumn(Column(_, Table(_, Schema(schema, _))))
      ) if (schema == s"${namespace}_${user}") => authQuery.allow()

      // All users may run queries
      case AuthQuery(
        _,
        AuthActionExecute,
        AuthResourceQuery(XQuery(_, _))
      ) => authQuery.allow()

      // Users may inspect query status and ownership of their own queries
      case AuthQuery(
        AuthIdUser(user),
        AuthActionRead,
        AuthResourceQuery(XQuery(_, Some(AuthIdUser(owner))))
      ) if user == owner => authQuery.allow()

      // Allow all users to cancel their own queries
      case AuthQuery(
        AuthIdUser(user),
        AuthActionDelete,
        AuthResourceQuery(XQuery(_, Some(AuthIdUser(owner))))
      ) if user == owner => authQuery.allow()

      // Allow execution of all functions
      case AuthQuery(
        _,
        AuthActionExecute,
        AuthResourceFunction(_)
      ) => authQuery.allow()

      // If we do not recognize the auth query combination, deny it
      case _ => authQuery.deny(messages.denyDefault)
    }
  }
}
