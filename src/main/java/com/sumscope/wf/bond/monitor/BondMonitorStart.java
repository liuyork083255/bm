package com.sumscope.wf.bond.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Component;

@Component
public class BondMonitorStart {
    private static final Logger logger = LoggerFactory.getLogger(BondMonitorStart.class);

    public static void main(String[] args) {
        SpringApplication.run(BondMonitorApplication.class, args).getBean(BondMonitorStart.class).start();
    }

    public void start(){
        logger.info("bond monitor start ...");
    }
}
