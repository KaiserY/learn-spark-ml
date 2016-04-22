package cn.shujuguan.controller

import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RequestParam, RestController}

/**
  * Created by kaisery on 2016/4/20.
  */
@RestController
class RootController {

  @RequestMapping(value = Array("/"), method = Array(RequestMethod.GET, RequestMethod.POST))
  def root(@RequestParam(value = "name", defaultValue = "World") name: String): String = {
    "Hello " + name
  }
}
