package cn.shujuguan.datatype

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http

import scala.concurrent.duration._

/**
  * Created by yueyang on 1/25/16.
  */
object SprayServer extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("on-spray-can")

  // setup ml
  DataTypeNaiveBayes.learn("hello world!")

  // create and start our service actor
  val service = system.actorOf(Props[DataTypeServiceActor], "data-type-service")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
}
