package compliance.engine.models

import compliance.engine.models.PolicyType.{Count, Speed, Time}

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

  def isCountMinimum: Boolean = rules.exists(rule => rule.policyType == Count && rule.minimum.nonEmpty)

  def doesVehicleEventMatch(vehicleEvent: VehicleEvent): Boolean =
    rules.exists(_.zones.intersect(vehicleEvent.zones).nonEmpty)
}

case class PolicyMatch(vehicleTypes: Set[String] = Set.empty)

object VehicleType {
  val Car = "Car"
  val Scooter = "Scooter"
  val Bike = "Bike"
}

object PolicyType {
  val Count = "Count"
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
                       inclusiveMinimum: Boolean = true,
                       inclusiveMaximum: Boolean = true,
                       zones: Set[UUID] = Set.empty
                     ) {
  def isValid: Boolean = policyType match {
    case Count =>
      units.isEmpty || units.exists(_.equalsIgnoreCase("devices"))
    case Speed =>
      units.exists(Set("mph", "kph").contains(_))
    case Time =>
      maximum.isDefined && units.exists(Set("seconds", "minutes", "hours").contains(_))
  }
}

object PolicyMatchType {
  val Matched = "Matched"
  val Unmatched = "Unmatched"
}

case class VehicleEventPolicyMatch(
                                    matchType: String,
                                    vehicleEvent: Option[VehicleEvent],
                                    policy: Policy
                                  )

object PolicyViolationNoticeType {
  val Started = "Started"
  val Ended = "Ended"
}

case class PolicyViolationNotice(
                                  policyId: UUID,
                                  ruleId: UUID,
                                  vehicleId: Option[UUID],
                                  slot: Option[Long],
                                  noticeType: String,
                                  timestamp: Long
                                )

case class PolicyMatchKey(
                           policyId: UUID,
                           vehicleId: UUID
                         )