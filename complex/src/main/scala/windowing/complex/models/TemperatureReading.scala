package windowing.complex.models

import java.util.UUID

case class TemperatureReading(
  stationId: UUID,
  temperature: Double,
  timestamp: Long
)
