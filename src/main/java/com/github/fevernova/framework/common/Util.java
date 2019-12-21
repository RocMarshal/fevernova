package com.github.fevernova.framework.common;


import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


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


    public static byte[] zip(byte[] bytes) {

        byte[] result = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();
            result = out.toByteArray();
        } catch (Exception e) {
            log.error("zip error : ", e);
            Validate.isTrue(false);
        }
        return result;
    }


    public static byte[] unzip(byte[] bytes) {

        byte[] result = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            ungzip.close();
            result = out.toByteArray();
        } catch (Exception e) {
            log.error("unzip error : ", e);
            Validate.isTrue(false);
        }
        return result;
    }


    public static List<String> splitStringWithFilter(String originalStr, String splitRegex, String filterRegex) {

        Validate.notNull(originalStr);
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


    public static void writeFile(String path, byte[] content) {

        try {
            File file = new File(path);
            Files.write(content, file);
        } catch (IOException e) {
            log.error("write file error : ", e);
            Validate.isTrue(false);
        }
    }


    public static String readFile(final File file) {

        try {
            return Files.readLines(file, Charset.defaultCharset(), new LineProcessor<String>() {


                StringBuffer sb = new StringBuffer();


                @Override public boolean processLine(String line) throws IOException {

                    sb.append(line);
                    return true;
                }


                @Override public String getResult() {

                    return sb.toString();
                }
            });
        } catch (Exception e) {
            log.error("read file error : ", e);
            Validate.isTrue(false);
        }
        return null;
    }


    public static void mkDir(File file) {

        if (!file.exists()) {
            mkDir(file.getParentFile());
            file.mkdir();
        }
    }


    public static Class findClass(String classname) {

        try {
            return Class.forName(classname);
        } catch (ClassNotFoundException e) {
            log.error("find class error : ", e);
            Validate.isTrue(false);
        }
        return null;
    }
}
