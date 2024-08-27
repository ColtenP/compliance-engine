package compliance.engine.sources

import compliance.engine.models.VehicleType.Scooter
import compliance.engine.models.{Policy, PolicyMatch, PolicyRule, PolicyType}

import java.util.UUID

object Constants {
  val CityParkZoneId: UUID = UUID.randomUUID()
  val DowntownZoneId: UUID = UUID.randomUUID()
  val MetroAreaZoneId: UUID = UUID.randomUUID()

  object Zones {
    val CityPark: Set[UUID] = Set(CityParkZoneId, DowntownZoneId, MetroAreaZoneId)
    val Downtown: Set[UUID] = Set(DowntownZoneId, MetroAreaZoneId)
    val MetroArea: Set[UUID] = Set(MetroAreaZoneId)
  }

  object Policies {
    private val ScooterPolicyMatch = PolicyMatch(
      vehicleTypes = Set(Scooter)
    )

    val DowntownScooterSpeedPolicy: Policy = Policy(
      id = UUID.randomUUID(),
      name = "Downtown Scooter Speed Policy",
      description = "Sets the speed limit for scooters in the Downtown Zone",
      policyType = PolicyType.Speed,
      rules = List(
        PolicyRule(
          id = UUID.randomUUID(),
          name = "Speed Limit",
          policyType = PolicyType.Speed,
          units = Some("kph"),
          minimum = None,
          maximum = Some(18.0),
          zones = Set(DowntownZoneId)
        )
      ),
      startsAt = Some(0),
      endsAt = None,
      matches = Some(ScooterPolicyMatch),
      supersedes = Set.empty
    )
  }
}
