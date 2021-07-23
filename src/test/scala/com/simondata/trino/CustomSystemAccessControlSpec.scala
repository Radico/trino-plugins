package com.simondata.trino

import com.simondata.UnitSpec
import com.simondata.util.XRay

class CustomSystemAccessControlSpec extends UnitSpec {
  "CustomSystemAccessControl" when {
    "defining custom methods" should {
      "override all methods declared on SystemAccessControl" in {
        println(s"Inspecting io.trino.spi.security.SystemAccessControl ...")
        val sacInfo = XRay.reflectClassName("io.trino.spi.security.SystemAccessControl")

        println(s"Inspecting com.simondata.trino.CustomSystemAccessControl ...")
        val customInfo = XRay.reflectClassName("com.simondata.trino.CustomSystemAccessControl")

        val customMap = customInfo.methods.map({ method =>
          method.name -> method
        }).toMap

        sacInfo.methods.foreach { sacMethod =>
          customMap.get(sacMethod.name) match {
            case None => assert(false, s"No method matching name '${sacMethod.name}''")
            case Some(customMethod) => {
              println(s"${sacMethod.name}()")
              sacMethod.parameters.zipAll(customMethod.parameters, null, null) foreach { case (sacParam, customParam) =>
                //assert(sacParam.name == customParam.name)
                assert(sacParam.valueType.name == customParam.valueType.name)
                val aliasStr = {
                  if (sacParam.name != customParam.name)
                    s" [ALIASED ${customParam.name}]"
                  else
                    ""
                }

                println(s"  ${sacParam.name}: ${sacParam.valueType.name}${aliasStr}")
              }
            }
          }
        }
      }
    }
  }
}
