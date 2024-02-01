package ca.yorku.eecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

@SuppressWarnings("restriction")
public class Handler implements HttpHandler {

	@Override
	public void handle(HttpExchange request) throws IOException {
		// TODO Auto-generated method stub
        String requestMethod = request.getRequestMethod();
        if ("GET".equalsIgnoreCase(requestMethod)) {
            handleGet(request);
        } else if ("PUT".equalsIgnoreCase(requestMethod)) {
            handlePut(request);
        } else {
        	sendString(request, "INTERNAL SERVER ERROR\n", 500);
        }
	}
	
	private void handlePut(HttpExchange request) throws IOException {
		try {
			URI uri = request.getRequestURI();
			String query = uri.getQuery();
			String command = getCmd(uri);
			Database db = new Database();
			
			try {
				switch (command) {
				case "addActor": db.addActor(query, request); 
				case "addMovie": db.addMovie(query, request);
				case "addRelationship": db.addRelationship(query, request);
				}
			}
			catch (Exception e){
				e.printStackTrace();
	        	sendString(request, "INTERNAL SERVER ERROR\n", 500);			
			}
			db.close();
        } 
		catch (Exception e) {
        	e.printStackTrace();
        	sendString(request, "INTERNAL SERVER ERROR\n", 500);
        }
	}
	
	private void handleGet(HttpExchange request) throws IOException {
		try {
			URI uri = request.getRequestURI();
			String query = uri.getQuery();
			String command = getCmd(uri);
			Database db = new Database();
			
			try {
				switch (command) {
				case "getActor": db.getActor(query, request);
				case "getMovie": db.getMovie(query, request);
				}
				
			}
			catch (Exception e){
				e.printStackTrace();
	        	sendString(request, "INTERNAL SERVER ERROR\n", 500);			
			}
			db.close();
        } 
		catch (Exception e) {
        	e.printStackTrace();
        	sendString(request, "INTERNAL SERVER ERROR\n", 500);
        }
	}
	
	private String getCmd(URI uri) {
		
		String cmd = uri.getPath();	
		return cmd.replace("/api/v1/", "");
		
	}
	
	private void sendString(HttpExchange request, String data, int restCode) 
			throws IOException {
		request.sendResponseHeaders(restCode, data.length());
        OutputStream os = request.getResponseBody();
        os.write(data.getBytes());
        os.close();
	}
	
	public static String convert(InputStream inputStream) throws IOException {
        
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }
	
	private static Map<String, String> splitQuery(String query) throws UnsupportedEncodingException {
        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
        }
        return query_pairs;
    }
	
	

}
