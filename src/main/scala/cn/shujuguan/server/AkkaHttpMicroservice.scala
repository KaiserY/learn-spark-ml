package cn.shujuguan.server

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, Materializer}
import cn.shujuguan.datatype.DataTypeNaiveBayes
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContextExecutor

trait Service {
  implicit val system: ActorSystem
  implicit val materializer: Materializer
  val logger: LoggingAdapter
  val routes = {
    logRequestResult("akka-http-microservice") {
      path("") {
        get {
          complete {
            logger.debug("Hello World")

            "Hello World"
          }
        }
      }
    }
  }

  implicit def executor: ExecutionContextExecutor

  def config: Config
}

object AkkaHttpMicroservice extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  override val config = ConfigFactory.load()
  override val logger = Logging(system, getClass)

  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))

  logger.info("Server started...")
}
