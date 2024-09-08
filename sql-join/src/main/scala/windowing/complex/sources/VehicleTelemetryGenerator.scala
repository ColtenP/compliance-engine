package windowing.complex.sources

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import windowing.complex.models.{VehicleEvent, VehicleTelemetry}

import java.time.Duration
import java.util.UUID
import scala.util.Random;

private class VehicleTelemetryGenerator extends GeneratorFunction[java.lang.Long, VehicleTelemetry] {
  @transient private lazy val random = new Random()

  override def map(sequence: java.lang.Long): VehicleTelemetry = {
    random.setSeed(sequence)

    new VehicleTelemetry(
      sequence % 100,
      random.nextDouble() * 50,
      random.nextDouble(),
      random.nextDouble(),
      System.currentTimeMillis()
    )
  }

}

object VehicleTelemetryGenerator {
  def toSource(env: StreamExecutionEnvironment): DataStream[VehicleTelemetry] = {
    env.fromSource(
      new DataGeneratorSource(
        new VehicleTelemetryGenerator,
        Long.MaxValue,
        RateLimiterStrategy.perSecond(20),
        TypeInformation.of(classOf[VehicleTelemetry])
      ),
      WatermarkStrategy
        .forBoundedOutOfOrderness[VehicleTelemetry](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[VehicleTelemetry] {
          def extractTimestamp(t: VehicleTelemetry, l: Long): Long = t.telemetry_timestamp
        }),
      "vehicle-events"
    )
  }
}