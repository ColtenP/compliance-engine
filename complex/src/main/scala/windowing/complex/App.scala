package windowing.complex

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import windowing.complex.models.{TemperatureMetric, TemperatureReading}
import windowing.complex.sources.TemperatureReadingGenerator
import windowing.complex.window.{MetricProcessWindowFunction, MetricReduceFunction}

object App {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val temperatureReadings = TemperatureReadingGenerator.create(env, 10000, numberOfStations = 100)

    val unaggregatedMetrics = temperatureReadings
      .map((reading: TemperatureReading) => TemperatureMetric(
        sensorId = reading.stationId,
        min = reading.temperature,
        max = reading.temperature,
        avg = reading.temperature,
        count = 1
      ))
      .name("unaggregated-metrics")

    val smallWindow = unaggregatedMetrics
      .keyBy((in: TemperatureMetric) => in.sensorId)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(new MetricReduceFunction, new MetricProcessWindowFunction)
      .name("one-minute-metrics")

    val mediumWindow = smallWindow
      .keyBy((in: TemperatureMetric) => in.sensorId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .reduce(new MetricReduceFunction, new MetricProcessWindowFunction)
      .name("five-minute-metrics")

    val largeWindow = mediumWindow
      .keyBy((in: TemperatureMetric) => in.sensorId)
      .window(TumblingEventTimeWindows.of(Time.minutes(60)))
      .reduce(new MetricReduceFunction, new MetricProcessWindowFunction)
      .name("one-hour-metrics")

    smallWindow.print("one-minute-metrics")
    mediumWindow.print("five-minute-metrics")
    largeWindow.print("one-hour-metrics")

    env.execute("Temperature Sensor Metrics")
  }
}
