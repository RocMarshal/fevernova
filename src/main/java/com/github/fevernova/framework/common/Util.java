package com.github.fevernova.framework.common;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Slf4j
public class Util {


    public static void sleepSec(long sec) {

        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            log.error("Util.sleep", e);
        }
    }


    public static void sleepMS(long ms) {

        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.error("Util.sleep", e);
        }
    }


    public static long nowSec() {

        return System.currentTimeMillis() / 1000;
    }


    public static long nowMS() {

        return System.currentTimeMillis();
    }


    public static String getHostname() {

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    public static List<String> splitStringWithFilter(String originalStr, String splitRegex, String filterRegex) {

        if (originalStr == null || splitRegex == null) {
            throw new RuntimeException(Util.class.getName() + "'s splitStringWithFilter method , originalStr or splitRegex is "
                                       + "null!");
        }
        return Arrays.stream(originalStr.split(splitRegex)).filter(str -> {
            if (StringUtils.isEmpty(str)) {
                return false;
            }
            if (StringUtils.isNotEmpty(filterRegex)) {
                return Pattern.matches(filterRegex, str);
            }
            return true;
        }).collect(Collectors.toList());
    }
}
