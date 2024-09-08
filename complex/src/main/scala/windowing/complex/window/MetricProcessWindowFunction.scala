package windowing.complex.window

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import windowing.complex.models.TemperatureMetric

import java.lang
import java.util.UUID
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class MetricProcessWindowFunction extends ProcessWindowFunction[TemperatureMetric, TemperatureMetric, UUID, TimeWindow] {

  def process(key: UUID, context: ProcessWindowFunction[TemperatureMetric, TemperatureMetric, UUID, TimeWindow]#Context, elements: lang.Iterable[TemperatureMetric], out: Collector[TemperatureMetric]): Unit = {
    elements
      .asScala
      .foreach { metric =>
        out.collect(
          metric.copy(
            windowStart = context.window().getStart,
            windowEnd = context.window().getEnd
          )
        )
      }
  }
}
