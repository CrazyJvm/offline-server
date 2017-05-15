package io.growing

import io.growing.offline.models.JobTableRecordStatus
import io.growing.offline.models.JobTableRecordStatus.JobTableRecordStatus
import slick.jdbc.PostgresProfile.api._
/**
  * Created by king on 4/13/17.
  */
package object offline {

  implicit val RecordStatusMapping = MappedColumnType.base[JobTableRecordStatus, String](
    e => e.toString,
    s => JobTableRecordStatus.withName(s)
  )

}
