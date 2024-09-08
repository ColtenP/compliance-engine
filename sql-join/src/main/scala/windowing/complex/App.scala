package windowing.complex

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row
import windowing.complex.models.{VehicleEvent, VehicleTelemetry}
import windowing.complex.sources.{VehicleEventGenerator, VehicleTelemetryGenerator}

object App {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(env)

    val vehicleEvents = VehicleEventGenerator.toSource(env)
    val vehicleTelemetry = VehicleTelemetryGenerator.toSource(env)

    val vehicleEventsRow = vehicleEvents
      .map((t: VehicleEvent) => Row.of(t.vehicle_id, t.event_type, t.event_timestamp))
      .returns(Types.ROW_NAMED(
        List("vehicle_id", "event_type", "event_timestamp").toArray,
        Types.LONG,
        Types.STRING,
        Types.LONG
      ))

    val vehicleTelemetryRow = vehicleTelemetry
      .map((t: VehicleTelemetry) => Row.of(t.vehicle_id, t.speed, t.latitude, t.longitude, t.telemetry_timestamp))
      .returns(Types.ROW_NAMED(
        List("vehicle_id", "speed", "latitude", "longitude", "telemetry_timestamp").toArray,
        Types.LONG,
        Types.DOUBLE,
        Types.DOUBLE,
        Types.DOUBLE,
        Types.LONG
      ))

    tenv.createTemporaryView(
      "vehicle_events",
      tenv.fromDataStream(
        vehicleEventsRow,
        Schema
          .newBuilder()
          .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build()
      )
    )

    tenv.createTemporaryView(
      "vehicle_telemetry",
      tenv.fromDataStream(
        vehicleTelemetryRow,
        Schema
          .newBuilder()
          .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build()
      )
    )

    tenv.from("vehicle_events").printSchema()
    tenv.from("vehicle_telemetry").printSchema()

    tenv.executeSql(
      """
        |WITH vehicle_event_telemetry AS (
        |    SELECT
        |        ve.vehicle_id,
        |        ve.event_type,
        |        vt.speed,
        |        vt.latitude,
        |        vt.longitude,
        |        ve.event_timestamp,
        |        vt.telemetry_timestamp
        |    FROM vehicle_events ve, vehicle_telemetry vt
        |    WHERE
        |        ve.vehicle_id = vt.vehicle_id AND
        |        ve.event_timestamp BETWEEN vt.telemetry_timestamp - 5000 AND vt.telemetry_timestamp
        |)
        |SELECT
        |    vet.*
        |FROM vehicle_event_telemetry vet
        |WHERE vet.speed IS NOT NULL
        |""".stripMargin)
  }
}
