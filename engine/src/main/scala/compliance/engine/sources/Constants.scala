package compliance.engine.sources

import compliance.engine.models.VehicleType.{Car, Scooter}
import compliance.engine.models.{Policy, PolicyMatch, PolicyRule, PolicyType}

import java.util.UUID

object Constants {
  val MetroAreaZoneId: UUID = UUID.fromString("0f808591-0b2b-4478-b1e5-8a7d41b5a4b1")
  val DowntownZoneId: UUID = UUID.fromString("e31d2281-6536-4385-a893-4e49612d286d")
  val SanFernandoValleyId: UUID = UUID.fromString("d7a6d6ca-84c4-450b-9a88-15ebfa74a02d")
  val SanGabrielValleyId: UUID = UUID.fromString("f7998d01-0026-496b-8634-7682cf71b2eb")
  val GatewayCitiesId: UUID = UUID.fromString("25706ceb-bfcf-4bcf-bfb1-3c5a3b957cbb")

  object Zones {
    val MetroArea: Set[UUID] = Set(MetroAreaZoneId)
  }

  object Policies {
    private val CarPolicyMatch = PolicyMatch(
      vehicleTypes = Set(Car)
    )

    val DowntownSpeedPolicy: Policy = Policy(
      id = UUID.randomUUID(),
      name = "Downtown Speed Policy",
      description = "Sets the speed limit for cars in the Downtown Zone",
      policyType = PolicyType.Speed,
      rules = List(
        PolicyRule(
          id = UUID.randomUUID(),
          name = "Speed Limit",
          policyType = PolicyType.Speed,
          units = Some("kph"),
          minimum = None,
          maximum = Some(100.0),
          zones = Set(DowntownZoneId)
        )
      ),
      startsAt = Some(0),
      endsAt = None,
      matches = Some(CarPolicyMatch),
      supersedes = Set.empty
    )
  }
}
