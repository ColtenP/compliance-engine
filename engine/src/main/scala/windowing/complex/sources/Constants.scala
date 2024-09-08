package compliance.engine.sources

import compliance.engine.models.{Policy, PolicyMatch, PolicyRule, PolicyType, VehicleType}

import java.util.UUID

object Constants {
  private val CityParkZoneId: UUID = UUID.randomUUID()
  private val DowntownZoneId: UUID = UUID.randomUUID()
  private val MetroAreaZoneId: UUID = UUID.randomUUID()

  object Zones {
    val CityPark: Set[UUID] = Set(CityParkZoneId, DowntownZoneId, MetroAreaZoneId)
    val Downtown: Set[UUID] = Set(DowntownZoneId, MetroAreaZoneId)
    val MetroArea: Set[UUID] = Set(MetroAreaZoneId)
  }

  object Policies {
    private val HeavyVehiclePolicyMatch = PolicyMatch(
      vehicleTypes = Set(VehicleType.HEAVY)
    )

    private val LightVehiclePolicyMatch = PolicyMatch(
      vehicleTypes = Set(VehicleType.LIGHT, VehicleType.BIKE)
    )

    private val IdlingPolicyMatch = PolicyMatch(
      isIdling = Some(true)
    )

    val DowntownHeavySpeedPolicy: Policy = Policy(
      id = UUID.randomUUID(),
      name = "Downtown Heavy Vehicle Speed Policy",
      description = "Sets the speed limit for heavy vehicles in the Downtown Zone",
      policyType = PolicyType.SPEED,
      rules = List(
        PolicyRule(
          id = UUID.randomUUID(),
          name = "Speed Limit",
          minimum = None,
          maximum = Some(30.0),
          zones = Set(DowntownZoneId)
        )
      ),
      profile = Some(HeavyVehiclePolicyMatch),
      supersedes = Set.empty
    )

    val DowntownLightSpeedPolicy: Policy = Policy(
      id = UUID.randomUUID(),
      name = "Downtown Light Vehicle Speed Policy",
      description = "Sets the speed limit for light vehicles and bikes in the Downtown Zone",
      policyType = PolicyType.SPEED,
      rules = List(
        PolicyRule(
          id = UUID.randomUUID(),
          name = "Speed Limit",
          minimum = None,
          maximum = Some(35.0),
          zones = Set(DowntownZoneId)
        )
      ),
      profile = Some(LightVehiclePolicyMatch),
      supersedes = Set.empty
    )

    val IdlingTimePolicy: Policy = Policy(
      id = UUID.randomUUID(),
      name = "Metro Idling Policy",
      description = "Sets the idling limit for all vehicles in the Metro Area",
      policyType = PolicyType.TIME,
      rules = List(

      ),
      profile = Some(IdlingPolicyMatch),
      supersedes = Set.empty
    )
  }
}
