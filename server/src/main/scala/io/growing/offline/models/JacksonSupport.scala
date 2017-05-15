package io.growing.offline.models

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import akka.http.javadsl.marshallers.jackson.Jackson
import scala.reflect.ClassTag

/**
  * Created by king on 1/6/17.
  */
/**
  * Automatic to and from JSON marshalling/unmarshalling usung an in-scope Jackon's ObjectMapper
  */
object JacksonSupport extends JacksonSupport {
  val defaultObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
}

/**
  * JSON marshalling/unmarshalling using an in-scope Jackson's ObjectMapper
  */
trait JacksonSupport {
  import JacksonSupport._

  private val jsonStringUnmarshaller =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(`application/json`)
      .mapWithCharset {
        case (ByteString.empty, _) => throw Unmarshaller.NoContentException
        case (data, charset)       => data.decodeString(charset.nioCharset.name)
      }

  /**
    * HTTP entity => `A`
    */
  implicit def jacksonUnmarshaller[A](
                                       implicit ct: ClassTag[A],
                                       objectMapper: ObjectMapper = defaultObjectMapper
                                     ): FromEntityUnmarshaller[A] = {
    jsonStringUnmarshaller.map(
      data => objectMapper.readValue(data, ct.runtimeClass).asInstanceOf[A]
    )
  }

  /**
    * `A` => HTTP entity
    */
  implicit def jacksonToEntityMarshaller[Object](
                                                  implicit objectMapper: ObjectMapper = defaultObjectMapper
                                                ): ToEntityMarshaller[Object] = {
    Jackson.marshaller[Object](objectMapper)
  }
}