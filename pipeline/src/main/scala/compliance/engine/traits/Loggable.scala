package compliance.engine.traits

import org.slf4j.LoggerFactory

trait Loggable {
  val LOGGER = LoggerFactory.getLogger(getClass)
}
