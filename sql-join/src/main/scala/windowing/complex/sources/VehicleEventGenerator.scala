package windowing.complex.sources;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import windowing.complex.models.VehicleEvent;

import java.time.Duration;

private class VehicleEventGenerator extends GeneratorFunction[java.lang.Long, VehicleEvent] {
  override def map(sequence: java.lang.Long): VehicleEvent =
    new VehicleEvent(
      sequence % 100,
      "DRIVING",
      System.currentTimeMillis()
    );
}

object VehicleEventGenerator {
  def toSource(env: StreamExecutionEnvironment): DataStream[VehicleEvent] = {
    env.fromSource(
      new DataGeneratorSource(
        new VehicleEventGenerator,
        Long.MaxValue,
        RateLimiterStrategy.perSecond(20),
        TypeInformation.of(classOf[VehicleEvent])
      ),
      WatermarkStrategy
        .forBoundedOutOfOrderness[VehicleEvent](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[VehicleEvent] {
          def extractTimestamp(t: VehicleEvent, l: Long): Long = t.event_timestamp
        }),
      "vehicle-events"
    )
  }
}