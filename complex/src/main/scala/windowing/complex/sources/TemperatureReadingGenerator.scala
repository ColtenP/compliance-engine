package windowing.complex.sources

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import windowing.complex.models.TemperatureReading

import java.lang
import java.time.Duration
import java.util.UUID
import scala.util.Random

class TemperatureReadingGenerator(stations: Int) extends GeneratorFunction[java.lang.Long, TemperatureReading] {
  @transient private lazy val stationIds = List.tabulate(stations)(index => new UUID(index, 0)).distinct
  @transient private lazy val random = new Random()

  def map(index: lang.Long): TemperatureReading = {
    random.setSeed(index)

    TemperatureReading(
      stationId = stationIds((index % stationIds.length).toInt),
      temperature = random.nextDouble() * 100,
      timestamp = System.currentTimeMillis() + ((index / stationIds.length) * 1000)
    )
  }
}

object TemperatureReadingGenerator {
  def create(env: StreamExecutionEnvironment, recordsPerSecond: Double = 10, numberOfEvents: Long = Long.MaxValue, numberOfStations: Int = 1000): DataStreamSource[TemperatureReading] = {
    val source = new DataGeneratorSource[TemperatureReading](
      new TemperatureReadingGenerator(numberOfStations),
      numberOfEvents,
      RateLimiterStrategy.perSecond(recordsPerSecond),
      TypeInformation.of(classOf[TemperatureReading])
    )

    env.fromSource(
      source,
      WatermarkStrategy
        .forBoundedOutOfOrderness[TemperatureReading](Duration.ofMinutes(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[TemperatureReading] {
          def extractTimestamp(event: TemperatureReading, l: Long): Long = event.timestamp
        }),
      "temperature-readings"
    )
  }
}
