package com.guohaoyu.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {
        //创建集合用于存放最终结果
        ArrayList<String> list = new ArrayList<>();

        //创建IK分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);
        Lexeme lexeme = ikSegmenter.next();

        while (lexeme!=null){
            String text = lexeme.getLexemeText();
            list.add(text);

            lexeme=ikSegmenter.next();
        }
        //返回结果数据
        return list;
    }
}
