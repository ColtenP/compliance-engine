package compliance.engine.traits

import org.slf4j.{Logger, LoggerFactory}

trait Loggable {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
}
