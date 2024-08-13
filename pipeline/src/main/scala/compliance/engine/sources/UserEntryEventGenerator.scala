package compliance.engine.sources

import compliance.engine.models.UserEntryEvent
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration
import java.util.UUID
import scala.util.Random

class UserEntryEventGenerator extends GeneratorFunction[java.lang.Long, UserEntryEvent] {
  @transient private lazy val Names: List[String] = List(
    "Alex", "Jordan", "Taylor", "Morgan", "Casey",
    "Riley", "Cameron", "Sydney", "Avery", "Peyton",
    "Dakota", "Skylar", "Quinn", "Reese", "Addison",
    "Blake", "Emerson", "Hayden", "Parker", "Rowan",
    "Sawyer", "Teagan", "Kendall", "Logan", "Dylan",
    "Carter", "Harper", "Finley", "Phoenix", "Emery",
    "Leighton", "River", "Charlie", "Robin", "Sage",
    "Ellis", "Remy", "Tatum", "Harlow", "Arden",
    "Ashton", "Blaine", "Cassidy", "Devon", "Ellison",
    "Frankie", "Greer", "Hollis", "Indigo", "Jesse",
    "Kai", "Lane", "Lennon", "Steve", "Nico",
    "Oakley", "Palmer", "Reagan", "Shiloh", "Tanner",
    "Tristan", "Vale", "Wren", "Zion", "Ash",
    "Beck", "Brady", "Chandler", "Dallas", "Ember",
    "Gale", "Harley", "Jordan", "Kaiya", "Lark",
    "Marlow", "Nova", "Onyx", "Pax", "Quill",
    "Ray", "Scout", "Toby", "Vaughn", "Wade",
    "Xander", "Yale", "Zane", "Arlo", "Blair",
    "Cade", "Darcy", "Elliot", "Flynn", "Grier",
    "Haze", "Ivy", "Jade", "Keaton", "Luca"
  ).distinct

  def map(t: java.lang.Long): UserEntryEvent =
    UserEntryEvent(
      id = UUID.randomUUID(),
      name = Names(Random.nextInt(Names.length)),
      timestamp = System.currentTimeMillis()
    )
}

object UserEntryEventGenerator {
  def create(env: StreamExecutionEnvironment, recordsPerSecond: Long = 10, numberOfEvents: Long = Long.MaxValue): DataStreamSource[UserEntryEvent] = {
    val source = new DataGeneratorSource[UserEntryEvent](
      new UserEntryEventGenerator,
      numberOfEvents,
      RateLimiterStrategy.perSecond(recordsPerSecond),
      TypeInformation.of(classOf[UserEntryEvent])
    )

    env.fromSource(
      source,
      WatermarkStrategy
        .forBoundedOutOfOrderness[UserEntryEvent](Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[UserEntryEvent] {
          def extractTimestamp(event: UserEntryEvent, l: Long): Long = event.timestamp
        }),
      "user-entry-events"
    )
  }
}