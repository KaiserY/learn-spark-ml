package cn.shujuguan

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

object App {
  def main(args: Array[String]) {
    SpringApplication.run(classOf[App], args: _*)
  }
}

@SpringBootApplication
class App
