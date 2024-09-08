package windowing.complex.models

import java.util.UUID

case class TemperatureMetric(
  sensorId: UUID,
  min: Double,
  max: Double,
  avg: Double,
  count: Long,
  windowStart: Long = Long.MinValue,
  windowEnd: Long = Long.MaxValue
)
