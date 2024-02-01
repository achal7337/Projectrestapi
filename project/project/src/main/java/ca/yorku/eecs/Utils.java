package ca.yorku.eecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.json.*;
import com.sun.net.httpserver.HttpExchange;

class Utils {        
    // use for extracting query params
    static Map<String, String> splitQuery(String query) throws UnsupportedEncodingException {
        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
        }
        return query_pairs;
    }
    
 // use for extracting JSON params
    static Map<String, String> splitJSON(String query) throws UnsupportedEncodingException, JSONException {
        query = new JSONObject(query).toString();
    	query = query.replace("{", "");
        query = query.replace("}", "");
        query = query.trim();
        System.out.println(query);
        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        String[] pairs = query.split(",");
        for (String pair : pairs) {
            int idx = pair.indexOf(":");
            query_pairs.put(URLDecoder.decode(removeQuotes(pair.substring(0, idx)), "UTF-8"), URLDecoder.decode(removeQuotes(pair.substring(idx+1)), "UTF-8"));
        }
        return query_pairs;
    }

    // one possible option for extracting JSON body as String
    static String convert(InputStream inputStream) throws IOException {
        
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    // another option for extracting JSON body as String
    static String getBody(HttpExchange he) throws IOException {
                InputStreamReader isr = new InputStreamReader(he.getRequestBody(), "utf-8");
            BufferedReader br = new BufferedReader(isr);
            
            int b;
            StringBuilder buf = new StringBuilder();
            while ((b = br.read()) != -1) {
                buf.append((char) b);
            }

            br.close();
            isr.close();
	    
        return buf.toString();
        }
    
    static String getCmd(URI uri) {
		
    	String cmd = uri.getPath();	
		cmd = cmd.replace("/api/v1/", "");
		cmd = cmd.replace("/api/v1", "");
		return cmd;
		
	}
    
    static String inQuotes(String s) { return "\"" + s + "\""; }
    
    static String removeQuotes(String s) { return s.replaceAll("\"", ""); }
    
    
    
}