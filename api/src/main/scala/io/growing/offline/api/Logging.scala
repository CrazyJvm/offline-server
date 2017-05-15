package io.growing.offline.api

import org.slf4j.LoggerFactory

/**
  * Created by king on 1/5/17.
  */
trait Logging {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
