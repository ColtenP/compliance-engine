package compliance.engine

import compliance.engine.process.SteveCounterMetric
import compliance.engine.sources.UserEntryEventGenerator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object App {
   def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val userEntries = UserEntryEventGenerator.create(env, 100)
      userEntries.map(new SteveCounterMetric())

      env.execute("User Entry Event Custom Metrics")
   }
}
