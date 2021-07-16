package front;

import com.fasterxml.jackson.databind.ObjectMapper;
import indexer.StopWords;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class WeatherDemo {

    public Map<String, Object> info;
    public String query;

    public WeatherDemo(String query) {
        this.query = query;
    }

    public String insertWetherInfo() {
        String[] keywords = Utils.normalizeInputStr(query);
        for (String word: keywords) {
            URL url = null;
            try {
                url = new URL("http://api.openweathermap.org/data/2.5/weather?q=" + word
                        + "&APPID=352389a0149b39b6d5aa229439cc6f5a");
            }
            catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
            }
            catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            }
            catch (Exception e) {
//                e.printStackTrace();
                continue;
            }
            StringBuilder res = new StringBuilder();
            String inputLine = "";
            try {
                while ((inputLine = reader.readLine()) != null) {
                    res.append(inputLine);
                }
                this.info = new ObjectMapper().readValue(res.toString(), HashMap.class);
            }
            catch (Exception e) {
                e.printStackTrace();
                return null;
            }

            if ((Integer)(this.info.get("cod")) == 200) {
                String cityId = String.valueOf(this.info.get("id"));
                
                StringBuilder sb = new StringBuilder();            
                sb.append("<div class=\"serp__sidebar\">\n" +
                        "        <div class=\"serp__sticky\">\n" +
                        "          <div class=\"serp__headline\">Weather Information</div>\n" +
                        "          <div id=\"openweathermap-widget-11\"></div>\n");
                sb.append("<script src='//openweathermap.org/themes/openweathermap/assets/vendor/owm/js/d3.min.js'></script><script>window.myWidgetParam ? window.myWidgetParam : window.myWidgetParam = [];  window.myWidgetParam.push({id: 11,cityid: '" + cityId + "',appid: 'ba04ce59a9e50f88eec2f48b59fa98e7',units: 'imperial',containerid: 'openweathermap-widget-11',  });  (function() {var script = document.createElement('script');script.async = true;script.charset = \"utf-8\";script.src = \"//openweathermap.org/themes/openweathermap/assets/vendor/owm/js/weather-widget-generator.js\";var s = document.getElementsByTagName('script')[0];s.parentNode.insertBefore(script, s);  })();</script>");
                sb.append("        </div>\n</div>\n");
                return sb.toString();
            }
        }
        return null;
    }

}
