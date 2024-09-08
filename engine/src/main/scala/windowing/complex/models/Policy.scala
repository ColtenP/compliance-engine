package compliance.engine.models

import java.util.UUID

case class Policy(
                   id: UUID,
                   name: String,
                   description: String,
                   policyType: PolicyType,
                   rules: List[PolicyRule],
                   profile: Option[PolicyMatch],
                   supersedes: Set[UUID] = Set.empty
                 ) {
  def isValid: Boolean = rules.nonEmpty

  def doesVehicleMatch(vehicleEvent: VehicleEvent): Boolean = {
    profile match {
      case Some(profile) =>
        (profile.vehicleTypes.isEmpty || profile.vehicleTypes.contains(vehicleEvent.vehicleType)) &&
          (profile.isIdling.isEmpty || profile.isIdling.contains(vehicleEvent.idling)) &&
          rules.exists(_.isVehicleInZones(vehicleEvent))
      case None => rules.exists(_.isVehicleInZones(vehicleEvent))
    }
  }
}

case class PolicyMatch(vehicleTypes: Set[VehicleType] = Set.empty, isIdling: Option[Boolean] = None)

case class PolicyRule(
                       id: UUID,
                       name: String,
                       minimum: Option[Double] = None,
                       maximum: Option[Double] = None,
                       zones: Set[UUID] = Set.empty
                     ) {
  def isValid: Boolean = minimum.nonEmpty || maximum.nonEmpty

  def isVehicleInZones(vehicleEvent: VehicleEvent): Boolean = zones.intersect(vehicleEvent.zones).nonEmpty
}

case class VehiclePolicyMatchUpdate(vehicleEvent: VehicleEvent, policy: Policy, matched: Boolean)
case class PolicyViolation(policyId: UUID, vehicleId: UUID, start: Long, end: Long)
case class PolicyMatchKey(policyId: UUID, vehicleId: UUID)
