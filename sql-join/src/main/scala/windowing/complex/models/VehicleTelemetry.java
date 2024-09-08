package windowing.complex.models;

public class VehicleTelemetry {
  public final Long vehicle_id;
  public final Double speed;
  public final Double latitude;
  public final Double longitude;
  public final Long telemetry_timestamp;

  public VehicleTelemetry(Long vehicleId, Double speed, Double latitude, Double longitude, Long telemetry_timestamp) {
    vehicle_id = vehicleId;
    this.speed = speed;
    this.latitude = latitude;
    this.longitude = longitude;
    this.telemetry_timestamp = telemetry_timestamp;
  }
}
