package com.example.hivecore.controller;

import com.alibaba.fastjson.JSON;
import com.example.hivecore.common.HiveConnection;
import com.example.hivecore.service.IHiveOprationService;
import com.example.hivecore.service.TestThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;

@RestController
@RequestMapping(value = "/hive")
public class HiveController {

    @Autowired
    IHiveOprationService hiveOprationService;

    @Value("${server.port}")
    private String port1;


    @RequestMapping(value = "/create")
    public String getHive(){
        boolean result = false;
        try {
            Connection connection = HiveConnection.getHiveConnection();
            result = hiveOprationService.CreateTable(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result ? "成功" : "失败";
    }

    @RequestMapping(value = "/createByName/{tableName}")
    public String getHive(@PathVariable String tableName){
        boolean result = false;

        try {
            //Connection connection = HiveConnection.getHiveConnection();
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + tableName + "~~~~~" + port1 );
            testThread();
            result = true ; //hiveOprationService.CreateTableByTableName(connection, tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result ? "成功" : "失败";
    }

    private static void testThread(){
        new Thread(new TestThread("a")).start();
        new Thread(new TestThread("b")).start();
    }

    @RequestMapping(value = "/insert")
    public String insertHive(){
        int result = 0;
        String str = "";
        try {
            Connection connection = HiveConnection.getHiveConnection();
            result = hiveOprationService.Insert(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (result == 0) {
            str = "失败";
        } else if (result == 1) {
            str = "成功";
        } else {
            str = "其他";
        }
        return str;
    }

    @RequestMapping(value = "/search")
    public String searchHive(){

        StringBuffer str = new StringBuffer();
        try {
            Connection connection = HiveConnection.getHiveConnection();
            ResultSet result = hiveOprationService.SearchTable(connection);

            while (result.next()) {
                System.out.println(result.getInt(1) + "\t" + result.getString(2));
                str.append(result.getInt(1)).append("\t").append(result.getString(2)).append("\n").append("\r");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return str.toString();
    }

    @RequestMapping(value = "/count/{tableName}")
    public String countHive(@PathVariable String tableName){
        String str = null;
        try {
            Connection connection = HiveConnection.getHiveConnection();
            int result = hiveOprationService.CountTable(connection, tableName);

            System.out.println("一共有："+ result + "条记录。");
            str = result + "";

        } catch (Exception e) {
            e.printStackTrace();
        }
        return str;
    }

    @RequestMapping(value = "/1")
    public String test(){
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL("http://localhost:8888/hive/sql");
            // 打开和URL之间的连接
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-type", "application/json");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            String str_json = JSON.toJSONString("{\"sql\":\"select cur_longitude,cur_latitude,cur_time,order_flag,kafka_time,work_order_id,work_order_no,emp_id from tb_druid_ecej_external_online_low order by kafka_time limit 5 offset 0\"\n" +
                    "}");
            out.print(str_json);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
        }
        return result;
    }
}
