package cn.shujuguan

import akka.event.NoLogging
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cn.shujuguan.server.Service
import org.scalatest._

class ServiceSpec extends FlatSpec with Matchers with ScalatestRouteTest with Service {
  override val logger = NoLogging

  override def testConfigSource = "akka.loglevel = WARNING"

  override def config = testConfig

  "Server" should "be OK" in {
    Get("/") ~> routes ~> check {
      status shouldBe OK
      contentType shouldBe `text/plain(UTF-8)`
      responseAs[String] shouldBe "Hello World"
    }
  }
}
