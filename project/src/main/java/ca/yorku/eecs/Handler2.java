package ca.yorku.eecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

@SuppressWarnings("restriction")
public class Handler2 implements HttpHandler {

	@Override
	public void handle(HttpExchange request) throws IOException {
		// TODO Auto-generated method stub
		try {
			String requestMethod = request.getRequestMethod();
            if (requestMethod.equals("GET")) {
                handleGet(request);
                System.out.println("get");
            } else if (requestMethod.equals("PUT")) {
            	handlePut(request);
            	System.out.println("put");
            }
            else sendString(request, "Unimplemented method\n", 501);
        } catch (Exception e) {
        	e.printStackTrace();
        	sendString(request, "Server error\n", 500);
        }
			
	}
	
	private void handleGet(HttpExchange request) throws IOException {
		
		try {
			URI uri = request.getRequestURI();
			String query = uri.getQuery();
			String command = Utils.getCmd(uri);
			Database db = new Database();
			
			try {
				//System.out.print(command);
				switch (command) {
				case "getActor": 	db.getActor(query, request);
									break;
				case "getMovie": 	db.getMovie(query, request);
									break;
				default: 			sendString(request, "BAD REQUEST\n", 400);
									break;
				}			
			}
			finally {
				db.close();
			}
        } 
		catch (Exception e) {
        	e.printStackTrace();
        	sendString(request, "INTERNAL SERVER ERROR\n", 500);
        }
		
	}
	
	private void handlePut(HttpExchange request) throws IOException {
		
		try {
			URI uri = request.getRequestURI();
			String command = Utils.getCmd(uri);
			Database2 db = new Database2();
			
			
			try {
				switch (command) {
				
				case "addActor": 		db.addActor(request); 
										System.out.println("addActor");
										break;
				case "addMovie": 		db.addMovie(request);
										break;
				case "addRelationship": db.addRelationship(request);
										break;
				default: 				sendString(request, "BAD REQUEST\n", 400);
										break;
				}
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				db.close();		
			}
        } 
		catch (Exception e) {
        	e.printStackTrace();
        	sendString(request, "INTERNAL SERVER ERROR\n", 500);
        }
		
	}
	
	private void sendString(HttpExchange request, String data, int restCode) 
			throws IOException {
		request.sendResponseHeaders(restCode, data.length());
        OutputStream os = request.getResponseBody();
        os.write(data.getBytes());
        os.close();
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
