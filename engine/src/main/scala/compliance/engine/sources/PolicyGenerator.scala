package compliance.engine.sources

import compliance.engine.models.Policy
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class PolicyGenerator extends GeneratorFunction[java.lang.Long, Policy] {
  def map(t: java.lang.Long): Policy =
    Constants.Policies.DowntownScooterSpeedPolicy
}

object PolicyGenerator {
  def create(env: StreamExecutionEnvironment, recordsPerSecond: Double = 10, numberOfEvents: Long = 1): DataStreamSource[Policy] = {
    val source = new DataGeneratorSource[Policy](
      new PolicyGenerator,
      numberOfEvents,
      RateLimiterStrategy.perSecond(recordsPerSecond),
      TypeInformation.of(classOf[Policy])
    )

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "policies")
  }
}