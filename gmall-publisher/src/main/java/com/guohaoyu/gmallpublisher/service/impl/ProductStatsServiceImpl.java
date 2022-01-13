package com.guohaoyu.gmallpublisher.service.impl;

import com.guohaoyu.gmallpublisher.mapper.ProductStatsMapper;
import com.guohaoyu.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;
    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }
}
