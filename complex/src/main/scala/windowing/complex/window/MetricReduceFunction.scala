package windowing.complex.window

import org.apache.flink.api.common.functions.ReduceFunction
import windowing.complex.models.TemperatureMetric

class MetricReduceFunction extends ReduceFunction[TemperatureMetric] {

  def reduce(agg: TemperatureMetric, curr: TemperatureMetric): TemperatureMetric = agg.copy(
    min = Math.min(agg.min, curr.min),
    max = Math.max(agg.max, curr.max),
    avg = ((agg.avg * agg.count) + (curr.avg * curr.count)) / (agg.count + curr.count),
    count = agg.count + curr.count
  )
}
