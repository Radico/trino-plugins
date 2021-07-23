package com.simondata.trino

import com.simondata.UnitSpec
import com.simondata.util.XRay

class CustomSystemAccessControlSpec extends UnitSpec {
  "CustomSystemAccessControl" when {
    "defining custom methods" should {
      "override all methods declared on SystemAccessControl" in {
        val sacInfo = XRay.reflectClassName("io.trino.spi.security.SystemAccessControl")
        val customInfo = XRay.reflectClassName("com.simondata.trino.CustomSystemAccessControl")

        val customMap = customInfo.methods.map({ method =>
          method.name -> method
        }).toMap

        sacInfo.methods.foreach { sacMethod =>
          customMap.get(sacMethod.name) match {
            case None => assert(false, s"No method matching name '${sacMethod.name}''")
            case Some(customMethod) => {
              sacMethod.parameters.zipAll(customMethod.parameters, null, null) foreach { case (sacParam, customParam) =>
                assert(sacParam.valueType.name == customParam.valueType.name)
              }
            }
          }
        }
      }
    }
  }
}
