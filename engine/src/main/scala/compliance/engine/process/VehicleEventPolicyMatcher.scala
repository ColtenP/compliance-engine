package compliance.engine.process

import compliance.engine.models.{Policy, SpeedPolicyMatchUpdate, TimePolicyMatchUpdate, VehicleEvent, VehiclePolicyMatchUpdate}
import compliance.engine.process.VehicleEventPolicyMatcher.{MATCHED_POLICY_STATE_DESCRIPTOR, POLICY_STATE_DESCRIPTOR, SPEED_POLICY_MATCHES, TIMER_STATE_DESCRIPTOR, TIME_POLICY_MATCHES, VEHICLE_EVENT_STATE_DESCRIPTOR}
import compliance.engine.traits.Loggable
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.{Collector, ExceptionUtils, OutputTag}

import java.time.Duration
import java.util.UUID
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}
import scala.collection.mutable
import scala.util.{Failure, Try}

class VehicleEventPolicyMatcher(deviceTimeout: Duration = Duration.ofDays(2))
  extends KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehiclePolicyMatchUpdate] with Loggable {
  @transient private var vehicleEventState: ValueState[VehicleEvent] = _
  @transient private var matchedPolicyState: ListState[UUID] = _
  @transient private var timerState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    vehicleEventState = getRuntimeContext.getState(VEHICLE_EVENT_STATE_DESCRIPTOR)
    matchedPolicyState = getRuntimeContext.getListState(MATCHED_POLICY_STATE_DESCRIPTOR)
    timerState = getRuntimeContext.getState(TIMER_STATE_DESCRIPTOR)
  }

  def processElement(
                      event: VehicleEvent,
                      ctx: KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehiclePolicyMatchUpdate]#ReadOnlyContext,
                      out: Collector[VehiclePolicyMatchUpdate]
                    ): Unit = {
    val matchedPolicies = ctx.getBroadcastState(POLICY_STATE_DESCRIPTOR)
      .immutableEntries()
      .asScala
      .map(_.getValue)
      .filter(_.doesVehicleEventMatch(event))

    // If this device has already matched with any policies previously, we need to unmatch it from policies that it
    // no longer matches with
    val existingMatchedPolicyIds = matchedPolicyState.get().asScala.toSet
    if (matchedPolicyState.get().asScala.nonEmpty) {
      existingMatchedPolicyIds
        .diff(matchedPolicies.map(_.id).toSet)
        .flatMap(unmatchedPolicyId => Option(ctx.getBroadcastState(POLICY_STATE_DESCRIPTOR).get(unmatchedPolicyId)))
        .foreach { unmatchedPolicy =>
          unmatchedPolicy.policyType match {
            case "Speed" => ctx.output(
              SPEED_POLICY_MATCHES,
              SpeedPolicyMatchUpdate(
                policyId = unmatchedPolicy.id,
                vehicleId = event.vehicleId,
                ruleId = unmatchedPolicy.rules.head.id,
                speed = event.speed,
                ruleMaximum = None,
                ruleMinimum = None,
                matched = false,
                eventTimestamp = event.timestamp
              )
            )
            case "Time" => ctx.output(
              TIME_POLICY_MATCHES,
              TimePolicyMatchUpdate(
                policyId = unmatchedPolicy.id,
                vehicleId = event.vehicleId,
                ruleId = unmatchedPolicy.rules.head.id,
                ruleMaximum = None,
                ruleMinimum = None,
                matched = false,
                eventTimestamp = event.timestamp
              )
            )
            case _ =>
          }
        }
    }

    // For all of the policies that this event matched, emit a matched event
    matchedPolicies.foreach { policy =>
      policy.policyType match {
        case "Speed" => ctx.output(
          SPEED_POLICY_MATCHES,
          SpeedPolicyMatchUpdate(
            policyId = policy.id,
            vehicleId = event.vehicleId,
            ruleId = policy.rules.head.id,
            speed = event.speed,
            ruleMaximum = policy.rules.head.maximum,
            ruleMinimum = policy.rules.head.minimum,
            matched = true,
            eventTimestamp = event.timestamp
          )
        )
        case "Time" => ctx.output(
          TIME_POLICY_MATCHES,
          TimePolicyMatchUpdate(
            policyId = policy.id,
            vehicleId = event.vehicleId,
            ruleId = policy.rules.head.id,
            ruleMaximum = policy.rules.head.maximum,
            ruleMinimum = policy.rules.head.minimum,
            matched = true,
            eventTimestamp = event.timestamp
          )
        )
        case _ =>
      }
    }

    vehicleEventState.update(event)
    matchedPolicyState.update(matchedPolicies.map(_.id).toList.asJava)
    Option(timerState.value()).foreach { existingTimer =>
      ctx.timerService().deleteEventTimeTimer(existingTimer)
    }
    ctx.timerService().registerEventTimeTimer(event.timestamp + deviceTimeout.toMillis)
  }

  def processBroadcastElement(
                               policy: Policy,
                               ctx: KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehiclePolicyMatchUpdate]#Context,
                               out: Collector[VehiclePolicyMatchUpdate]
                             ): Unit = {
    if (!policy.isValid) {
      LOGGER.error(s"Ignoring invalid policy ${policy.id}")
      return
    }

    ctx.getBroadcastState(POLICY_STATE_DESCRIPTOR).put(policy.id, policy)

    val matchedVehicles = mutable.Set[UUID]()

    ctx.applyToKeyedState(VEHICLE_EVENT_STATE_DESCRIPTOR, (vehicleId: UUID, state: ValueState[VehicleEvent]) => {
      Option(state.value()).filter(policy.doesVehicleEventMatch).foreach { vehicleEvent =>
        matchedVehicles.add(vehicleId)

        out.collect(
          VehiclePolicyMatchUpdate(
            vehicleEvent = vehicleEvent,
            policy = policy,
            matched = true
          )
        )
      }
    })

    ctx.applyToKeyedState(MATCHED_POLICY_STATE_DESCRIPTOR, (vehicleId: UUID, state: ListState[UUID]) => {
      if (matchedVehicles.contains(vehicleId)) {
        state.add(policy.id)
      }
    })
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehiclePolicyMatchUpdate]#OnTimerContext,
                        out: Collector[VehiclePolicyMatchUpdate]
                      ): Unit = {
    // If a device has not had any activity in the duration of deviceTimeout, then clear its state and unmatch its
    // matched policies
    Try {
      timerState.clear()

      Option(vehicleEventState.value()).foreach { vehicleEvent =>
        matchedPolicyState.get().asScala.foreach { matchedPolicyId =>
          Try(ctx.getBroadcastState(POLICY_STATE_DESCRIPTOR).get(matchedPolicyId)).toOption.foreach { policy =>
            out.collect(VehiclePolicyMatchUpdate(
              vehicleEvent = vehicleEvent,
              policy = policy,
              matched = false
            ))
          }
        }
      }

      vehicleEventState.clear()
      matchedPolicyState.clear()
    } match {
      case Failure(exception) =>
        LOGGER.error("Error while processing vehicle event policy match timer: {}", ExceptionUtils.stringifyException(exception))
      case _ =>
    }
  }
}

object VehicleEventPolicyMatcher {
  val POLICY_STATE_DESCRIPTOR = new MapStateDescriptor[UUID, Policy](
    "PolicyState",
    TypeInformation.of(classOf[UUID]),
    TypeInformation.of(classOf[Policy])
  )

  private val VEHICLE_EVENT_STATE_DESCRIPTOR = new ValueStateDescriptor[VehicleEvent](
    "VehicleEventState",
    TypeInformation.of(classOf[VehicleEvent])
  )

  private val MATCHED_POLICY_STATE_DESCRIPTOR = new ListStateDescriptor[UUID](
    "MatchedPolicyState",
    TypeInformation.of(classOf[UUID])
  )

  private val TIMER_STATE_DESCRIPTOR = new ValueStateDescriptor[Long](
    "TimerState",
    TypeInformation.of(classOf[Long])
  )

  val SPEED_POLICY_MATCHES =
    new OutputTag[SpeedPolicyMatchUpdate]("SpeedPolicyMatch", TypeInformation.of(classOf[SpeedPolicyMatchUpdate]))
  val TIME_POLICY_MATCHES =
    new OutputTag[TimePolicyMatchUpdate]("TimePolicyMatch", TypeInformation.of(classOf[TimePolicyMatchUpdate]))
}