package com.guohaoyu.gmalllogger.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
        //System.out.println(jsonStr);
        Logger logger = LoggerFactory.getLogger(LoggerController.class);
        logger.info(jsonStr);
        kafkaTemplate.send("ods_base_log",jsonStr);
        return "sucess";
    }
}
