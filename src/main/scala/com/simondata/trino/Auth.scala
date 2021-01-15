package com.simondata.trino

import java.nio.file.AccessDeniedException
import java.security.Principal

import io.trino.spi.security.SystemSecurityContext

// Auth Identities
sealed abstract class AuthId(category: String) {
  def name: String
  override def toString(): String = s"id:${category}:${name}"
}
case class AuthIdPrincipal(name: String) extends AuthId("principal")
case class AuthIdUser(name: String) extends AuthId("user")
case object AuthIdUnknown extends AuthId("anonymous") {
  val name = "unknown"
}
object AuthId {
  def of(securityContext: SystemSecurityContext): AuthId = AuthIdUser(securityContext.getIdentity.getUser)
  def of(principal: Principal): AuthId = AuthIdUser(principal.getName)
  def unknown: AuthId = AuthIdUnknown
}

// Auth Actions
sealed abstract class AuthAction(name: String) {
  override def toString(): String = s"action:${name}"
}
case object AuthActionGrant extends AuthAction("grant")
case object AuthActionRevoke extends AuthAction("revoke")
case object AuthActionCreate extends AuthAction("create")
case object AuthActionRead extends AuthAction("read")
case object AuthActionUpdate extends AuthAction("update")
case object AuthActionDelete extends AuthAction("delete")
case object AuthActionExecute extends AuthAction("execute")

// Auth Resources
sealed trait AuthResource {
  def resource: Resource
  override def toString(): String = s"resource:${resource.category}:${resource}"
}
case class AuthResourceSession(resource: Session = Session.anyProp) extends AuthResource
case class AuthResourceCatalog(resource: Catalog) extends AuthResource
case class AuthResourceSchema(resource: Schema) extends AuthResource
case class AuthResourceTable(resource: Table) extends AuthResource
case class AuthResourceView(resource: Table) extends AuthResource
case class AuthResourceColumn(resource: Column) extends AuthResource
case class AuthResourceRecord(resource: Record) extends AuthResource
case class AuthResourceFunction(resource: XFunction) extends AuthResource
case class AuthResourceProcedure(resource: XProcedure) extends AuthResource
case class AuthResourceQuery(resource: XQuery) extends AuthResource
case object AuthResourceSystemInfo extends AuthResource {
  def resource: Resource = SystemInfo
}

/**
 * Represents a question regarding whether an identity is permitted to apply an action to a resource.
 *
 * @param id the identity acting on the resource
 * @param action the action being requested
 * @param resource the resource to which the action will be applied
 */
case class AuthQuery(
  id: AuthId,
  action: AuthAction,
  resource: AuthResource
) {
  def deny(reason: String): AuthDenied = AuthDenied(this, DenialString(reason))
  def deny(reason: AccessDeniedException): AuthDenied = AuthDenied(this, DenialException(reason))
  def allow(): AuthAllowed = AuthAllowed(this)
}


// Auth Outcomes
sealed abstract class AuthResult {
  def query: AuthQuery
}
case class AuthAllowed(query: AuthQuery) extends AuthResult
case class AuthDenied(query: AuthQuery, reason: DenialReason) extends AuthResult

sealed trait DenialReason {
  def exception: AccessDeniedException
  override def toString: String = exception.getMessage
}
case class DenialString(reason: String) extends DenialReason {
  val exception = new AccessDeniedException(reason)
}
case class DenialException(reason: AccessDeniedException) extends DenialReason {
  val exception = reason
}

/**
 * A request to filter a requested set of auth queries resulting in only that subset which is permitted.
 *
 * @param authQueries the Set of AuthQuery which should be filtered
 */
case class FilterRequest(id: AuthId, authQueries: List[AuthQuery])

// Filter Outcome
case class FilterResult(request: FilterRequest, allowed: List[AuthResult], denied: List[AuthResult])
