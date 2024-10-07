package compliance.engine.models

import java.util.UUID

case class VehicleEvent(
  id: UUID,
  vehicleId: UUID,
  vehicleType: String,
  zones: Set[UUID],
  speed: Double,
  timestamp: Long
)
