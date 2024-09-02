package compliance.engine.process

import compliance.engine.models.{Policy, PolicyMatchType, VehicleEvent, VehicleEventPolicyMatch}
import compliance.engine.process.VehicleEventPolicyMatcher.{MATCHED_POLICY_STATE_DESCRIPTOR, POLICY_STATE_DESCRIPTOR, TIMER_STATE_DESCRIPTOR, VEHICLE_EVENT_STATE_DESCRIPTOR}
import compliance.engine.traits.Loggable
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.{Collector, ExceptionUtils}

import java.time.Duration
import java.util.UUID
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}
import scala.collection.mutable
import scala.util.{Failure, Try}

class VehicleEventPolicyMatcher(deviceTimeout: Duration = Duration.ofDays(2))
  extends KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehicleEventPolicyMatch] with Loggable {
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
                      ctx: KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehicleEventPolicyMatch]#ReadOnlyContext,
                      out: Collector[VehicleEventPolicyMatch]
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
          out.collect(
            VehicleEventPolicyMatch(
              matchType = PolicyMatchType.Unmatched,
              vehicleEvent = Some(event),
              policy = unmatchedPolicy
            )
          )
        }
    }

    // For all of the policies that this event matched, emit a matched event
    matchedPolicies.foreach { policy =>
      out.collect(
        VehicleEventPolicyMatch(
          matchType = PolicyMatchType.Matched,
          vehicleEvent = Some(event),
          policy = policy
        )
      )
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
                               ctx: KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehicleEventPolicyMatch]#Context,
                               out: Collector[VehicleEventPolicyMatch]
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
          VehicleEventPolicyMatch(
            matchType = PolicyMatchType.Matched,
            vehicleEvent = Some(vehicleEvent),
            policy = policy
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
                        ctx: KeyedBroadcastProcessFunction[UUID, VehicleEvent, Policy, VehicleEventPolicyMatch]#OnTimerContext,
                        out: Collector[VehicleEventPolicyMatch]
                      ): Unit = {
    // If a device has not had any activity in the duration of deviceTimeout, then clear its state and unmatch its
    // matched policies
    Try {
      timerState.clear()

      Option(vehicleEventState.value()).foreach { vehicleEvent =>
        matchedPolicyState.get().asScala.foreach { matchedPolicyId =>
          Try(ctx.getBroadcastState(POLICY_STATE_DESCRIPTOR).get(matchedPolicyId)).toOption.foreach { policy =>
            out.collect(VehicleEventPolicyMatch(
              matchType = PolicyMatchType.Unmatched,
              vehicleEvent = Some(vehicleEvent),
              policy = policy
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
}