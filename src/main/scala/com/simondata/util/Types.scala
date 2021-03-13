/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.util

import java.util.{Collection, Optional}
import scala.jdk.CollectionConverters._

object Types {
  def toOption[T](optional: Optional[T]): Option[T] = optional.map[Option[T]]((v: T) => Some(v)).orElse(None)
  def toOptional[T](option: Option[T]): Optional[T] = option match {
    case None => Optional.empty
    case Some(v) => Optional.of(v)
  }
  def toSet[T](collection: Collection[T]): Set[T] = collection.asScala.toSet
  def toList[T](collection: Collection[T]): List[T] = collection.asScala.toList
  def zip[A,B](a: Option[A], b: Option[B]): Option[(A,B)] = (a, b) match {
    case (Some(x), Some(y)) => Some((x, y))
    case _ => None
  }
}
