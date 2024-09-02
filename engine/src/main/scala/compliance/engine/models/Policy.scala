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
  def isValid: Boolean = {
    if (rules.isEmpty) return false
    Console.println(rules.length)

    rules.forall(rule => rule.isValid && rule.policyType == policyType)
  }

  def doesVehicleEventMatch(vehicleEvent: VehicleEvent): Boolean =
    rules.exists(_.zones.intersect(vehicleEvent.zones).nonEmpty)

  def findFirstRuleInViolation(vehicleEvent: Option[VehicleEvent]): Option[UUID] =
    policyType match {
      case "Speed" =>
        rules.find { rule =>
          rule.maximum.exists(_ >= vehicleEvent.map(_.speed).getOrElse(Double.MinValue)) ||
            rule.minimum.exists(_ <= vehicleEvent.map(_.speed).getOrElse(Double.MaxValue))
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
                       inclusiveMinimum: Boolean = true,
                       inclusiveMaximum: Boolean = true,
                       zones: Set[UUID] = Set.empty
                     ) {
  def isValid: Boolean = policyType match {
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