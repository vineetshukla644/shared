package demo.microservice;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/demo")
public class DemoController {



    @Autowired
    DemoController() {

    }

    @GetMapping("/hello")
    public String all() {
        return "Hi there";
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message) {

        System.out.println("Hi There");
    }
}


