package windowing.complex

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object App {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(env)

    tenv.executeSql(
      """
        |CREATE TABLE vehicle_events (
        |  vehicle_id BIGINT,
        |  event_type VARCHAR(32),
        |  event_timestamp TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '20',
        |  'fields.vehicle_id.kind' = 'sequence',
        |  'fields.vehicle_id.start' = '1',
        |  'fields.vehicle_id.end' = '100'
        |)
        |""".stripMargin)

    tenv.executeSql(
      """
        |CREATE TABLE vehicle_telemetry (
        |  vehicle_id BIGINT,
        |  speed DOUBLE,
        |  latitude DOUBLE,
        |  longitude DOUBLE,
        |  telemetry_timestamp TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '20',
        |  'fields.vehicle_id.kind' = 'sequence',
        |  'fields.vehicle_id.start' = '1',
        |  'fields.vehicle_id.end' = '100'
        |);
        |""".stripMargin)

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
        |        ve.event_timestamp BETWEEN vt.telemetry_timestamp - INTERVAL '5' SECOND AND vt.telemetry_timestamp
        |)
        |SELECT
        |    vet.*
        |FROM vehicle_event_telemetry vet
        |WHERE vet.speed IS NOT NULL;
        |""".stripMargin)
  }
}
