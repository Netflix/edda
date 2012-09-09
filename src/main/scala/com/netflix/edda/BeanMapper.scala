package com.netflix.edda

trait BeanMapper {
    def apply(obj: Any): Any
    def addKeyMapper( pf: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] )
    def addObjMapper( pf: PartialFunction[AnyRef,AnyRef] )
}
