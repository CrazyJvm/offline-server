package io.growing.offline.models

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.slf4j.LoggerFactory

/**
  * Created by king on 12/21/16.
  */
@JsonIgnoreProperties
case class JobConfig(name: String, jobInfo: JobInfo, submitInfo: SubmitInfo)