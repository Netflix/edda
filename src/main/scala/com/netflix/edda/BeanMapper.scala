package com.netflix.edda

trait BeanMapperComponent {
    val beanMapper: BeanMapper;
}

trait BeanMapper {
    def apply(obj: Any): Any
}
