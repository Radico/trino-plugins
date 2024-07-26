/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

import com.simondata.UnitSpec

class NamespacedAuthSpec extends UnitSpec {
  val namespace = "simon"
  val auth: TrinoAuth = new NamespacedAuth(namespace)

  /* The `sac` instance of CustomSystemAccessControl is referenced in comments in most test sections.
   * Its purpose is to permit jumping (within IntelliJ) to the relevant section in CustomSystemAccessControl
   * for a set of assertions. Uncomment the line, then Ctrl/Cmd+click on the function name to jump to its
   * definition. You can return via alt+shift+left (navigate back in history)
   */
  lazy val sac = new CustomSystemAccessControl(auth)

  def session(property: Option[String] = None, value: Option[String] = None) = AuthResourceSession(Session(property, value))
  def catalog(catalog: String) = AuthResourceCatalog(Catalog(catalog))
  def schema(schema: String, catalog: String) = AuthResourceSchema(Schema(schema, Catalog(catalog)))
  def table(table: String, schema: String, catalog: String) = AuthResourceTable(
    Table(table, Schema(schema, Catalog(catalog)))
  )
  def column(column: String, table: String, schema: String, catalog: String) = AuthResourceColumn(
    Column(column, Table(table, Schema(schema, Catalog(catalog))))
  )

  def assertQueryResult(query: AuthQuery)(mapper: AuthQuery => AuthResult): Unit = {
    val expectedResult = mapper(query)
    assert(auth.authorize(query) == expectedResult)
  }

  "NamespacedAuth" when {
    "authorizing" should {
      "allow setting session `user` property as non-user, admin, or root" in {
        //sac.checkCanSetUser()
        //sac.checkCanImpersonateUser()

        assertQueryResult(
          AuthQuery(AuthIdUnknown, AuthActionUpdate, session(Some("user")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session(Some("user")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session(Some("user")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("bob"), AuthActionUpdate, session(Some("user")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session(Some("user")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session(Some("user")))
        ) {
          _.allow()
        }
      }

      "allow setting session `user` property to the current user" in {
        //sac.checkCanImpersonateUser()
        //sac.checkCanSetUser()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("user"), Some("bob")))
        ) {
          _.allow()
        }
      }

      "disallow setting session `user` property to a different user" in {
        //sac.checkCanImpersonateUser()
        //sac.checkCanSetUser()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bill"), AuthActionUpdate, session(Some("identity"), Some("ted")))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "allow setting session `query_max_execution_time` property" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session(Some("query_max_execution_time")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session(Some("query_max_execution_time")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("bob"), AuthActionUpdate, session(Some("query_max_execution_time")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session(Some("query_max_execution_time")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session(Some("query_max_execution_time")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("query_max_execution_time")))
        ) {
          _.allow()
        }
      }

      "allow setting session `query_max_stage_count` property" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session(Some("query_max_stage_count")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session(Some("query_max_stage_count")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("bob"), AuthActionUpdate, session(Some("query_max_stage_count")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session(Some("query_max_stage_count")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session(Some("query_max_stage_count")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("query_max_stage_count")))
        ) {
          _.allow()
        }
      }

      "allow setting session `spill_enabled` property" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session(Some("spill_enabled")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session(Some("spill_enabled")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("bob"), AuthActionUpdate, session(Some("spill_enabled")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session(Some("spill_enabled")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session(Some("spill_enabled")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("spill_enabled")))
        ) {
          _.allow()
        }
      }

      "allow setting session `spill_order_by` property" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session(Some("spill_order_by")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session(Some("spill_order_by")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("bob"), AuthActionUpdate, session(Some("spill_order_by")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session(Some("spill_order_by")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session(Some("spill_order_by")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("spill_order_by")))
        ) {
          _.allow()
        }
      }

      "allow setting session `spill_window_operator` property" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session(Some("spill_window_operator")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session(Some("spill_window_operator")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("bob"), AuthActionUpdate, session(Some("spill_window_operator")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session(Some("spill_window_operator")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session(Some("spill_window_operator")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("spill_window_operator")))
        ) {
          _.allow()
        }
      }

      "allow setting session properties as `admin`" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, session())
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, session())
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session())
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session())
        ) {
          _.allow()
        }
      }

      "deny setting misc. session properties as user" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session())
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("identity")))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionUpdate, session(Some("query_max_memory")))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny all actions for anonymous" in {
        assertQueryResult(
          AuthQuery(AuthIdUnknown, AuthActionRead, catalog("hive"))
        ) {
          _.deny(TrinoAuth.messages.denyAnonymous)
        }

        assertQueryResult(
          AuthQuery(AuthIdUnknown, AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyAnonymous)
        }
      }

      "deny all actions for catalogs other than hive for non-admin users" in {
        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, catalog("hill"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, catalog("hill"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("boss"), AuthActionRead, catalog("hill"))
        ) {
          _.deny(TrinoAuth.messages.denyCatalog)
        }
      }

      "allow session actions for a principal" in {
        //sac.checkCanSetCatalogSessionProperty()
        //sac.checkCanSetSystemSessionProperty()
        //sac.checkCanImpersonateUser()
        //sac.checkCanSetUser()

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionUpdate, session())
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionUpdate, session())
        ) {
          _.allow()
        }
      }

      "deny non-session actions for a principal except root" in {
        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionRead, catalog("hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionRead, catalog("hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("root"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdPrincipal("admin"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "allow all users to read hive.information_schema.columns" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("columns", "information_schema", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, table("columns", "information_schema", "hive"))
        ) {
          _.allow()
        }
      }

      "allow all users to read hive.information_schema.tables" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("tables", "information_schema", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, table("tables", "information_schema", "hive"))
        ) {
          _.allow()
        }
      }

      "allow all users to read hive.information_schema.views" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("views", "information_schema", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, table("views", "information_schema", "hive"))
        ) {
          _.allow()
        }
      }

      "deny non-admin users performing grants" in {
        //sac.checkCanGrantExecuteFunctionPrivilege()
        //sac.checkCanGrantTablePrivilege()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("boss"), AuthActionGrant, catalog("hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("boss"), AuthActionGrant, schema("information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("boss"), AuthActionGrant, table("views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("boss"), AuthActionGrant, column("id", "views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny users performing non-reads against hive.information_schema.columns" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("columns", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, table("columns", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, table("columns", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny users performing non-reads against hive.information_schema.tables" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("tables", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, table("tables", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, table("tables", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny users performing non-reads against hive.information_schema.views" in {
        //sac.checkCanSetColumnComment()
        //sac.checkCanDropColumn()
        //sac.checkCanAddColumn()
        //sac.checkCanSetTableComment()
        //sac.checkCanDropTable()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, table("views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, table("views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, column("id", "views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, column("id", "views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, column("id", "views", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny non-admin users reading from non-excluded hive.information_schema.*" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("applicable_roles", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("enabled_roles", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("roles", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("schemata", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("table_privileges", "information_schema", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "allow all users to perform CRUD in schema hive.trino_shared" in {
        //sac.checkCanCreateTable()
        //sac.checkCanDropTable()
        //sac.checkCanCreateView()
        //sac.checkCanDropView()
        //sac.checkCanAddColumn()
        //sac.checkCanDropColumn()
        //sac.checkCanSetColumnComment()
        //sac.checkCanSetTableComment()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, table("tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, table("tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }
      }

      "allow all users perform CRUD in schema hive.trino_shared_dev" in {
        //sac.checkCanCreateTable()
        //sac.checkCanDropTable()
        //sac.checkCanCreateView()
        //sac.checkCanDropView()
        //sac.checkCanAddColumn()
        //sac.checkCanDropColumn()
        //sac.checkCanSetColumnComment()
        //sac.checkCanSetTableComment()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, table("tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, table("tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }
      }

      "deny non-admin users performing CRUD in unsupported hive.trino_shared_*" in {
        //sac.checkCanCreateTable()
        //sac.checkCanDropTable()
        //sac.checkCanCreateView()
        //sac.checkCanDropView()
        //sac.checkCanAddColumn()
        //sac.checkCanDropColumn()
        //sac.checkCanSetColumnComment()
        //sac.checkCanSetTableComment()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, table("tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, table("tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, table("tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "allow all users to read metadata from schema hive.trino_shared" in {
        //sac.checkCanShowTables()
        //sac.checkCanShowCreateTable()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, schema("trino_shared", "hive"))
        ) {
          _.allow()
        }
      }

      "allow all users to alter temporary tables in schema hive.trino_shared" in {
        //sac.checkCanAddColumn()
        //sac.checkCanDropColumn()
        //sac.checkCanSetColumnComment()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, column("timestamp", "tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, column("timestamp", "tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, column("timestamp", "tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, column("timestamp", "tmp_ted", "trino_shared", "hive"))
        ) {
          _.allow()
        }
      }

      "allow all users to alter temporary tables in hive.trino_shared_dev" in {
        //sac.checkCanAddColumn()
        //sac.checkCanDropColumn()
        //sac.checkCanSetColumnComment()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, column("timestamp", "tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, column("timestamp", "tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, column("timestamp", "tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, column("timestamp", "tmp_ted", "trino_shared_dev", "hive"))
        ) {
          _.allow()
        }
      }

      "deny non-admin users altering temporary tables in unsupported hive.trino_shared_*" in {
        //sac.checkCanAddColumn()
        //sac.checkCanDropColumn()
        //sac.checkCanSetColumnComment()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, column("timestamp", "tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, column("timestamp", "tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, column("timestamp", "tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, column("timestamp", "tmp_ted", "trino_shared_other", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny alterations to the hive.trino_shared schema" in {
        //sac.checkCanCreateSchema()
        //sac.checkCanDropSchema()
        //sac.checkCanSetSchemaAuthorization()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, schema("trino_shared", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, schema("trino_shared", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, schema("trino_shared", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny alterations to the hive.trino_shared_dev schema" in {
        //sac.checkCanCreateSchema()
        //sac.checkCanDropSchema()
        //sac.checkCanSetSchemaAuthorization()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionCreate, schema("trino_shared_dev", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, schema("trino_shared_dev", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, schema("trino_shared_dev", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "allow all users to select from tables in their namespaced schema" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("teds_table", s"${namespace}_ted", "hive"))
        ) {
          _.allow()
        }
      }

      "deny users selecting from tables outside of their namespaced schema" in {
        //sac.checkCanSelectFromColumns()
        //sac.filterColumns()
        //sac.filterTables()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, table("teds_table", s"${namespace}_ned", "hive"))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "default to denying when an unsupported auth-query structures are encountered" in {
      }
    }

    def applyFilterRequest(request: FilterRequest)(action: FilterResult => Unit): Unit = {
      action(auth.filter(request))
    }

    def assertFilterResult(request: FilterRequest)(
      expectedAllowed: List[AuthResult],
      expectedDenied: List[AuthResult]
    ): Unit = {
      applyFilterRequest(request) { case FilterResult(_, allowed, denied) =>
        assert(allowed == expectedAllowed)
        assert(denied == expectedDenied)
      }
    }

    "filtering" should {
      "partition allowed/denied into complimentary sets" in {
        val userRead = AuthQuery(AuthIdIdentity("bob"), AuthActionRead, catalog("hive"))
        val anonRead = AuthQuery(AuthIdUnknown, AuthActionRead, catalog("hive"))
        val request = FilterRequest(AuthIdUnknown, userRead :: anonRead :: Nil)

        assertFilterResult(request)(
          userRead.allow() :: Nil,
          anonRead.deny(TrinoAuth.messages.denyAnonymous) :: Nil
        )
      }

      "have an empty denied set when all are allowed" in {
        val readSchema = AuthQuery(AuthIdIdentity("bob"), AuthActionRead, schema(s"${namespace}_bob", "hive"))
        val readTable = AuthQuery(AuthIdIdentity("bob"), AuthActionRead, schema(s"${namespace}_bob", "hive"))
        val request = FilterRequest(AuthIdUnknown, readSchema :: readTable :: Nil)

        assertFilterResult(request)(
          readSchema.allow() :: readTable.allow() :: Nil,
          Nil
        )
      }

      "have an empty allowed set when all are denied" in {
        val readSchema = AuthQuery(AuthIdUnknown, AuthActionRead, schema(s"${namespace}_bob", "hive"))
        val readTable = AuthQuery(AuthIdUnknown, AuthActionRead, schema(s"${namespace}_bob", "hive"))
        val request = FilterRequest(AuthIdUnknown, readSchema :: readTable :: Nil)

        assertFilterResult(request)(
          Nil,
          readSchema.deny(TrinoAuth.messages.denyAnonymous) :: readTable.deny(TrinoAuth.messages.denyAnonymous) :: Nil
        )
      }
    }

    "checking query status" should {
      "allow root to inspect any user's query" in {
        //sac.checkCanViewQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.allow()
        }
      }

      "allow admin to inspect any user's query" in {
        //sac.checkCanViewQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.allow()
        }
      }

      "allow a user to inspect their own queries" in {
        //sac.checkCanViewQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.allow()
        }
      }

      "deny non admin users from inspecting another user's queries" in {
        //sac.checkCanViewQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("bob"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }
    }

    "executing queries" should {
      "allow all users to execute queries" in {
        //sac.checkCanExecuteQuery()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("anybody"), AuthActionExecute, AuthResourceQuery(XQuery(None, None)))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("anybody"), AuthActionExecute, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("root")))))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("anybody"), AuthActionExecute, AuthResourceQuery(XQuery(Some("1"), None)))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("anybody"), AuthActionExecute, AuthResourceQuery(XQuery(Some("1"), Some(AuthIdIdentity("root")))))
        ) {
          _.allow()
        }
      }
    }

    "killing queries" should {
      "allow root to kill any users' queries" in {
        //sac.checkCanKillQuery()
        //sac.checkCanKillQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("root")))))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("admin")))))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.allow()
        }
      }

      "allow admin to kill any non-admin users' queries" in {
        //sac.checkCanKillQuery()
        //sac.checkCanKillQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("admin")))))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.allow()
        }
      }

      "allow users to kill their own queries" in {
        //sac.checkCanKillQuery()
        //sac.checkCanKillQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("ted")))))
        ) {
          _.allow()
        }
      }

      "deny admin killing root's queries" in {
        //sac.checkCanKillQuery()
        //sac.checkCanKillQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("root")))))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }

      "deny users killing any other users' queries" in {
        //sac.checkCanKillQuery()
        //sac.checkCanKillQueryOwnedBy()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("root")))))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("admin")))))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionDelete, AuthResourceQuery(XQuery(None, Some(AuthIdIdentity("bob")))))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }
    }

    "enforcing function execution" should {
      "allow all users to execute all functions" in {
        //sac.checkCanExecuteFunction()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionExecute, AuthResourceFunction(XFunction("very_scary")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionExecute, AuthResourceFunction(XFunction("very_scary")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionExecute, AuthResourceFunction(XFunction("very_scary")))
        ) {
          _.allow()
        }
      }
    }

    "enforcing procedure execution" should {
      "allow admin users to call procedures" in {
        //sac.checkCanExecuteProcedure()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionExecute, AuthResourceProcedure(XProcedure("nothing_scary")))
        ) {
          _.allow()
        }

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionExecute, AuthResourceProcedure(XProcedure("nothing_scary")))
        ) {
          _.allow()
        }
      }

      "deny non-admin users calling procedures" in {
        //sac.checkCanExecuteProcedure()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionExecute, AuthResourceProcedure(XProcedure("nothing_scary")))
        ) {
          _.deny(TrinoAuth.messages.denyDefault)
        }
      }
    }

    "enforcing system resources" should {
      "allow root to read system info" in {
        //sac.checkCanReadSystemInformation()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionRead, AuthResourceSystemInfo)
        ) {
          _.allow()
        }
      }

      "allow root to update system info" in {
        //sac.checkCanWriteSystemInformation()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("root"), AuthActionUpdate, AuthResourceSystemInfo)
        ) {
          _.allow()
        }
      }

      "allow admin to read system info" in {
        //sac.checkCanReadSystemInformation()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionRead, AuthResourceSystemInfo)
        ) {
          _.allow()
        }
      }

      "deny admin updating system info" in {
        //sac.checkCanWriteSystemInformation()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("admin"), AuthActionUpdate, AuthResourceSystemInfo)
        ) {
          _.deny(TrinoAuth.messages.denyUpdateSystemInfo)
        }
      }

      "deny other users reading system info" in {
        //sac.checkCanReadSystemInformation()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionRead, AuthResourceSystemInfo)
        ) {
          _.deny(TrinoAuth.messages.denyUpdateSystemInfo)
        }
      }

      "deny other users updating system info" in {
        //sac.checkCanWriteSystemInformation()

        assertQueryResult(
          AuthQuery(AuthIdIdentity("ted"), AuthActionUpdate, AuthResourceSystemInfo)
        ) {
          _.deny(TrinoAuth.messages.denyUpdateSystemInfo)
        }
      }
    }
  }
}
