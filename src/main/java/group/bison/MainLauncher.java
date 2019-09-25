package group.bison;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by BSONG on 2019/6/30.
 */
@SpringBootApplication(scanBasePackages = "group.bison")
@EnableAutoConfiguration
public class MainLauncher {

    public static void main(String[] args) {
        try {
            SpringApplication springApplication = new SpringApplication(MainLauncher.class);
            springApplication.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
   