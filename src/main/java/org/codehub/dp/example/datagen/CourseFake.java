package org.codehub.dp.example.datagen;


import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class CourseFake {

    private static final AtomicInteger idCount = new AtomicInteger(1);
    private static final Random r = new Random();

    private static final Map<String, Double> javaCoursePrice = ImmutableMap.<String, Double>builder()
            .put("微服务实战", 8888.0)
            .put("Java实战", 8888.0)
            .build();
    private static final Map<String, Double> bigDataCoursePrice = ImmutableMap.<String, Double>builder()
            .put("Hadoop实战", 4444.0)
            .put("Flink实战", 5555.0)
            .put("Spark实战", 6666.0)
            .put("云原生实战", 7777.0)
            .put("大数据运维实战", 8888.0)
            .build();

    public static String toJson() {
        int categoryId = r.nextInt(2) + 1;
        String courseName = getCourseName(categoryId);
        double courseMoney = getCourseMoney(categoryId, courseName);
        return "{"
                + "\"categoryId\": " + categoryId + ","
                + "\"description\": \"" + "test" + "\","
                + "\"id\": " + idCount.getAndAdd(1) + ","
                + "\"ip\": \"" + getRandomIp() + "\","
                + "\"name\": \"" + courseName + "\","
                + "\"money\": " + courseMoney + ","
                + "\"os\": \"" + getRandomOs() + "\","
                + "\"status\": " + r.nextInt(3) + ","
                + "\"ts\": " + System.currentTimeMillis()
                + "}";
    }

    private static String getRandomOs() {
        String[] osNames = new String[]{"Mac Os", "Android", "Harmony OS", "IOS"};
        return osNames[r.nextInt(osNames.length)];
    }

    private static double getCourseMoney(int categoryId, String courseName) {
        return categoryId == 1
                ? bigDataCoursePrice.get(courseName)
                : javaCoursePrice.get(courseName);
    }

    /*
     * 根据categoryId获取
     */
    private static String getCourseName(int categoryId) {
        String[] bigDataCourseNames = bigDataCoursePrice.keySet().toArray(new String[0]);
        String[] javaCourseNames = javaCoursePrice.keySet().toArray(new String[0]);
        return categoryId == 1
                ? bigDataCourseNames[r.nextInt(bigDataCourseNames.length)]
                : javaCourseNames[r.nextInt(javaCourseNames.length)];
    }

    /*
     * 随机生成国内IP地址
     */
    private static String getRandomIp() {

        // ip范围
        int[][] range = { { 607649792, 608174079 },// 36.56.0.0-36.63.255.255
                { 1038614528, 1039007743 },// 61.232.0.0-61.237.255.255
                { 1783627776, 1784676351 },// 106.80.0.0-106.95.255.255
                { 2035023872, 2035154943 },// 121.76.0.0-121.77.255.255
                { 2078801920, 2079064063 },// 123.232.0.0-123.235.255.255
                { -1950089216, -1948778497 },// 139.196.0.0-139.215.255.255
                { -1425539072, -1425014785 },// 171.8.0.0-171.15.255.255
                { -1236271104, -1235419137 },// 182.80.0.0-182.92.255.255
                { -770113536, -768606209 },// 210.25.0.0-210.47.255.255
                { -569376768, -564133889 }, // 222.16.0.0-222.95.255.255
        };

        Random rd = new Random();
        int index = rd.nextInt(10);
        return num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
    }

    /*
     * 将十进制转换成ip地址
     */
    private static String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";

        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = b[0] + "." + b[1] + "." + b[2] + "." + b[3];

        return x;
    }

}
