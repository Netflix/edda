package com.netflix.edda

object Utils {
    def toObjects(args: Any*): Array[AnyRef] = {
        args.map(arg => arg match {
            case v: Char    => v.asInstanceOf[java.lang.Character]
            case v: Byte    => v.asInstanceOf[java.lang.Byte]
            case v: Short   => v.asInstanceOf[java.lang.Short]
            case v: Int     => v.asInstanceOf[java.lang.Integer]
            case v: Long    => v.asInstanceOf[java.lang.Long]
            case v: Float   => v.asInstanceOf[java.lang.Float]
            case v: Double  => v.asInstanceOf[java.lang.Double]
            case v: Boolean => v.asInstanceOf[java.lang.Boolean]
            case (v: Any, f: String) => f.format(v)
            case v: AnyRef  => v
        }).toArray[AnyRef]
    }
}
