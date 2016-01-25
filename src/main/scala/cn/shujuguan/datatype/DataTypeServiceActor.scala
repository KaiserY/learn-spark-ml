package cn.shujuguan.datatype

import akka.actor.Actor
import spray.http.MediaTypes._
import spray.routing._

/**
  * Created by yueyang on 1/25/16.
  */
class DataTypeServiceActor extends Actor with DataTypeService {

  def actorRefFactory = context

  def receive = runRoute(route)
}

trait DataTypeService extends HttpService {

  val route = {
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
          val prediction = DataTypeNaiveBayes.learn(text)
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
