package com.sumscope.wf.bond.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@EnableScheduling
@SpringBootApplication
public class BondMonitorApplication {

	public static void main(String[] args) {
		SpringApplication.run(BondMonitorApplication.class, args);
	}
}
