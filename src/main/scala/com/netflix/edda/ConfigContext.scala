package com.netflix.edda

import java.util.Properties

trait ConfigContext {
  def config: Properties
}
