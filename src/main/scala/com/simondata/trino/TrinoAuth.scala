package com.simondata.trino

/**
 * Custom authorization implementations should extend this trait.
 * The default example is NamespacedAuth.
 */
trait TrinoAuth {
  /**
   * Evaluate an AuthQuery. Is the identity permitted to apply the action to the resource.
   *
   * @param authQuery the AuthQuery which should be evaluated
   *
   * @return AuthAllow if permitted, otherwise AuthDeny
   */
  def authorize(authQuery: AuthQuery): AuthResult

  /**
   * Partition auth queries into passed/failed sets. Maintaining the sets on both sides provides
   * context for debugging/logging and permits the caller to use either side as the filtered set.
   *
   * @param filterRequest the auth queries to filter
   *
   * @return a FilterResult which partitions the auth queries into sets of allowed/denied
   */
  def filter(filterRequest: FilterRequest): FilterResult = {
    val (allowed, denied) = filterRequest.authQueries
      .map(authorize(_))
      .partition(_ match {
        case _: AuthAllowed => true
        case _: AuthDenied => false
      })

    FilterResult(filterRequest, allowed, denied)
  }
}

object TrinoAuth {
  object messages {
    val denyAnonymous = "Anonymous actions are not permitted."
    val denyCatalog= "Only the hive catalog may be used."
    val denyUpdateSystemInfo = "Only root may update system info."
    val denyDefault = "Uncategorized auth-query combination."
  }
}
