package cn.shujuguan.controller

import org.springframework.web.bind.annotation.{RequestMapping, RestController}

/**
  * Created by kaisery on 2016/4/20.
  */
@RestController
class RootController {

  @RequestMapping(Array("/"))
  def root: String = {
    "Hello World!"
  }
}
