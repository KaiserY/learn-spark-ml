package cn.shujuguan

import cn.shujuguan.controller.RootController
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, ShouldMatchers}
import org.springframework.boot.test.SpringApplicationConfiguration
import org.springframework.mock.web.MockServletContext
import org.springframework.test.context.TestContextManager
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.springframework.test.context.web.WebAppConfiguration
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders._
import org.springframework.test.web.servlet.result.MockMvcResultMatchers._
import org.springframework.test.web.servlet.setup.MockMvcBuilders._

/**
  * Created by kaisery on 2016/4/20.
  */
@RunWith(classOf[SpringJUnit4ClassRunner])
@SpringApplicationConfiguration(classes = Array(classOf[MockServletContext]))
@WebAppConfiguration
class SpringBootSpec extends FlatSpec with ShouldMatchers {

  var mvcMock: MockMvc = standaloneSetup(new RootController).build()

  new TestContextManager(this.getClass()).prepareTestInstance(this)

  "User" should "be able to visit the home page" in {
    mvcMock.perform(get("/"))
      .andExpect(status.isOk)
      .andExpect(content.string("Hello World"))
    mvcMock.perform(post("/").param("name", "World"))
      .andExpect(status.isOk)
      .andExpect(content.string("Hello World"))
  }
}
