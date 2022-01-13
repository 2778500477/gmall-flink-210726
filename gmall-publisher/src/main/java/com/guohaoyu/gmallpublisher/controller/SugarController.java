package com.guohaoyu.gmallpublisher.controller;

import com.guohaoyu.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

//@Controller
@RestController
@RequestMapping("/api/sugar")
public class SugarController {
    @Autowired
    ProductStatsService productStatsService;
    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date==0){
          date=getToday();
        }

        BigDecimal gmv = productStatsService.getGmv(date);
        return "{\n" +
                "    \"status\": 0,\n" +
                "    \"msg\": \"\",\n" +
                "    \"data\": "+gmv+"\n" +
                "}\n";
    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        return Integer.parseInt(sdf.format(ts));
    }
}
