package compliance.engine.models

import compliance.engine.models.PolicyType.{Speed, Time}

import java.util.UUID

case class Policy(
                   id: UUID,
                   name: String,
                   description: String,
                   policyType: String,
                   rules: List[PolicyRule],
                   startsAt: Option[Long],
                   endsAt: Option[Long],
                   matches: Option[PolicyMatch],
                   supersedes: Set[UUID] = Set.empty
                 ) {
  def isValid: Boolean = rules.nonEmpty && rules.forall(rule => rule.isValid && rule.policyType == policyType)

  def doesVehicleEventMatch(vehicleEvent: VehicleEvent): Boolean =
    rules.exists(_.zones.intersect(vehicleEvent.zones).nonEmpty)

  def findFirstRuleInViolation(vehicleEvent: VehicleEvent): Option[UUID] =
    policyType match {
      case "Speed" =>
        rules.find { rule =>
          rule.maximum.exists(_ <= vehicleEvent.speed) ||
            rule.minimum.exists(_ >= vehicleEvent.speed)
        }.map(_.id)
      case _ => None
    }
}

case class PolicyMatch(vehicleTypes: Set[String] = Set.empty)

object VehicleType {
  val Car = "Car"
  val Scooter = "Scooter"
  val Bike = "Bike"
}

object PolicyType {
  val Speed = "Speed"
  val Time = "Time"
}

case class PolicyRule(
                       id: UUID,
                       name: String,
                       policyType: String,
                       units: Option[String] = None,
                       minimum: Option[Double] = None,
                       maximum: Option[Double] = None,
                       zones: Set[UUID] = Set.empty
                     ) {
  def isValid: Boolean = policyType match {
    case Speed =>
      units.exists(Set("mph", "kph").contains(_))
    case Time =>
      maximum.isDefined && units.exists(Set("seconds", "minutes", "hours").contains(_))
  }
}

case class VehiclePolicyMatchUpdate(vehicleEvent: VehicleEvent, policy: Policy, matched: Boolean)

case class PolicyViolation(
                            policyId: UUID,
                            vehicleId: UUID,
                            start: Long,
                            end: Long
                          )

case class PolicyMatchKey(
                           policyId: UUID,
                           vehicleId: UUID
                         )