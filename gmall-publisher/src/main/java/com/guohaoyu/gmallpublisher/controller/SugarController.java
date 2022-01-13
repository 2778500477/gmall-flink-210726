package com.guohaoyu.gmallpublisher.controller;

import com.guohaoyu.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

//@Controller
@RestController
public class SugarController {
    @Autowired
    ProductStatsService productStatsService;
    @RequestMapping("/api/sugar/gmv")
    public String getGmv(@RequestParam("date") int date){
        BigDecimal gmv = productStatsService.getGmv(date);
        return "{\n" +
                "    \"status\": 0,\n" +
                "    \"msg\": \"\",\n" +
                "    \"data\": "+gmv+"\n" +
                "}\n";
    }
}
