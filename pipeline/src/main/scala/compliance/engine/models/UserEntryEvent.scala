package compliance.engine.models

import java.util.UUID

case class UserEntryEvent(
    id: UUID,
    name: String,
    timestamp: Long
)
