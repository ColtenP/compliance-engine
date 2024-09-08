package windowing.complex.models;

public class VehicleEvent {
  public final Long vehicle_id;
  public final String event_type;
  public final Long event_timestamp;

  public VehicleEvent(Long vehicle_id, String event_type, Long event_timestamp) {
    this.vehicle_id = vehicle_id;
    this.event_type = event_type;
    this.event_timestamp = event_timestamp;
  }
}
