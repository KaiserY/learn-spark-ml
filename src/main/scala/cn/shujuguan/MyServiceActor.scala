package cn.shujuguan

import akka.actor.Actor
import spray.http.MediaTypes._
import spray.routing._

/**
  * Created by yueyang on 12/29/15.
  */
// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class MyServiceActor extends Actor with MyService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(myRoute)
}


// this trait defines our service behavior independently from the service actor
trait MyService extends HttpService {

  val myRoute = {
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          // XML is marshalled to `text/xml` by default, so we simply override here
          complete {
            <html>
              <body>
                <h1>Hello World!</h1>
              </body>
            </html>
          }
        }
      }
    } ~
      pathPrefix("spark" / Rest) { content =>
        get {
          val text = java.net.URLDecoder.decode(content, "UTF-8")
          val prediction = MLDataType.learn(text)
          respondWithMediaType(`text/html`) {
            // XML is marshalled to `text/xml` by default, so we simply override here
            complete {
              <html>
                <body>
                  <pre>
                    <h1>
                      {prediction}
                    </h1>
                  </pre>
                </body>
              </html>
            }
          }
        }
      }
  }
}
