package compliance.engine.sources

import compliance.engine.models.VehicleEvent
import compliance.engine.models.VehicleType.{Bike, Car, Scooter}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration
import java.util.UUID
import scala.util.Random

class VehicleEventGenerator extends GeneratorFunction[java.lang.Long, VehicleEvent] {
  @transient private lazy val Vehicles: List[(UUID, String)] = List.tabulate(1000) { _ =>
    (UUID.randomUUID(), Random.nextInt(100) match {
      case value if value < 50 => Scooter
      case value if value < 70 => Bike
      case _ => Car
    })
  }.distinct

  def map(t: java.lang.Long): VehicleEvent = {
    val vehicle = Vehicles(Random.nextInt(Vehicles.length))

    VehicleEvent(
      id = UUID.randomUUID(),
      vehicleId = vehicle._1,
      vehicleType = vehicle._2,
      zones = Random.nextInt(100) match {
        case value if value < 50 => Constants.Zones.CityPark
        case value if value < 80 => Constants.Zones.Downtown
        case _ => Constants.Zones.MetroArea
      },
      speed = Random.nextDouble() * 25.0,
      timestamp = System.currentTimeMillis()
    )
  }
}

object VehicleEventGenerator {
  def create(env: StreamExecutionEnvironment, recordsPerSecond: Double = 10, numberOfEvents: Long = Long.MaxValue): DataStreamSource[VehicleEvent] = {
    val source = new DataGeneratorSource[VehicleEvent](
      new VehicleEventGenerator,
      numberOfEvents,
      RateLimiterStrategy.perSecond(recordsPerSecond),
      TypeInformation.of(classOf[VehicleEvent])
    )

    env.fromSource(
      source,
      WatermarkStrategy
        .forBoundedOutOfOrderness[VehicleEvent](Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[VehicleEvent] {
          def extractTimestamp(event: VehicleEvent, l: Long): Long = event.timestamp
        }),
      "vehicle-events"
    )
  }
}