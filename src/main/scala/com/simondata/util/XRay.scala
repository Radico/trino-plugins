/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.util

import java.lang.reflect.{Field, Method, Parameter}

import play.api.libs.json.{JsArray, JsObject, JsString, JsValue}

/**
 * A utility which will generate a report on the structure of
 * a class and its properties via reflection.
 */
object XRay {
  def reflectObject(obj: Object): ClassInfo = reflectClass(obj.getClass)
  def reflectClassName(className: String): ClassInfo = reflectClass(Class.forName(className))
  def reflectClass(cls: Class[_]): ClassInfo = ClassInfo.forClass(cls)
  def getCallerName(): Option[String] = getMethodName(2)
  def getMethodName(): Option[String] = getMethodName(1)
  def getMethodName(depth: Int): Option[String] = {
    val state = new Throwable("")
    val trace = state.getStackTrace
    val method = trace.lift(depth + 1)
    method.map(_.getMethodName)
  }
}

class ParameterInfo(parameter: Parameter) {
  lazy val name: String = parameter.getName
  lazy val valueType: ClassInfo = ClassInfo.forClass(parameter.getType)

  def asJson: JsValue = JsObject(Seq(
    "name" -> JsString(name),
    "type" -> JsString(valueType.name)
  ))
}

class MethodInfo(method: Method) {
  lazy val name: String = method.getName
  lazy val parameters: Seq[ParameterInfo] = method.getParameters.map(new ParameterInfo(_)).toSeq
  lazy val returnType: ClassInfo = ClassInfo.forClass(method.getReturnType)

  def asJson: JsValue = JsObject(Seq(
    "name" -> JsString(name),
    "parameters" -> JsArray(parameters.map(_.asJson)),
    "returnType" -> JsString(returnType.name)
  ))
}

class FieldInfo(field: Field) {
  lazy val name: String = field.getName
  lazy val valueType: ClassInfo = ClassInfo.forClass(field.getType)

  def asJson: JsValue = JsObject(Seq(
    "name" -> JsString(name),
    "type" -> JsString(valueType.name)
  ))
}

class ClassInfo(cls: Class[_]) {
  lazy val name: String = cls.getCanonicalName
  lazy val fields: Set[FieldInfo] = Set.from(cls.getFields) map { new FieldInfo(_) }
  lazy val methods: Set[MethodInfo] = Set.from(cls.getMethods) map { new MethodInfo(_)}
  lazy val children: Set[ClassInfo] = Set.from(cls.getClasses) map { ClassInfo.forClass(_) }

  def asJson: JsValue = JsObject(Seq(
    "name" -> JsString(name),
    "fields" -> JsArray(fields.map(_.asJson).toSeq),
    "methods" -> JsArray(methods.map(_.asJson).toSeq),
    "children" -> JsArray(children.map(_.asJson).toSeq)
  ))

  override def hashCode(): Int = name.hashCode
  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[ClassInfo] match {
      case false => false
      case true => name.equals(obj.asInstanceOf[ClassInfo].name)
    }
  }
}

object ClassInfo {
  private var infoCache: Map[String, ClassInfo] = Map.empty

  def forClass(cls: Class[_]): ClassInfo = {
    infoCache.get(cls.getCanonicalName) match {
      case Some(info) => info
      case None => {
        val info = new ClassInfo(cls)
        infoCache += (cls.getCanonicalName -> info)
        info
      }
    }
  }
}
