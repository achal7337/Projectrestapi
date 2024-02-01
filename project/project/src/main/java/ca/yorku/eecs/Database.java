package ca.yorku.eecs;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.*;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.json.*;

import com.sun.net.httpserver.HttpExchange;

@SuppressWarnings("restriction")
public class Database {
	
	private Driver driver;
	private String uriDb;
	
	public Database() {
		uriDb = "bolt://localhost:7687"; // may need to change if you used a different port for your DBMS
		Config config = Config.builder().withoutEncryption().build();
		driver = GraphDatabase.driver(uriDb, AuthTokens.basic("neo4j","12345678"), config);
	}
	
	public void addActor(HttpExchange request) throws IOException {
		String query = Utils.convert(request.getRequestBody());
		System.out.println(query);
		
		
		try (Session session = driver.session()) {
			JSONObject jsonObj = new JSONObject(query);
			String name = jsonObj.getString("name");
			String actorId = jsonObj.getString("actorId");

			if (jsonObj.has("name")
					&& jsonObj.has("actorId")
					&& !name.equals("")
					&& !actorId.equals("")
					&& !containsActorId(actorId)) {
				String cmd =  "MERGE (a:actor {name: $x, actorId: $y})";
				session.writeTransaction(tx -> tx.run(cmd, parameters("x", name, "y", actorId)));
				sendString(request, "OK", 200);
			}
			else sendString(request, "BAD REQUEST", 400);
		
		}
		catch (Exception e) {
			e.printStackTrace();
		}
							
	}
	
	public void addMovie(HttpExchange request) throws IOException {
		String query = Utils.convert(request.getRequestBody());
		System.out.println(query);
	
		try (Session session = driver.session()) {
			JSONObject jsonObj = new JSONObject(query);
			String name = jsonObj.getString("name");
			String movieId = jsonObj.getString("movieId");
			
			System.out.print(containsMovieId(movieId));
			
			if (jsonObj.has("name")
					&& jsonObj.has("movieId") 
					&& !containsMovieId(movieId)) {
				String cmd =  "MERGE (a:movie {name: $x, movieId: $y})";
				session.writeTransaction(tx -> tx.run(cmd, parameters("x", name, "y", movieId)));
				sendString(request, "OK", 200);
			}
			else sendString(request, "BAD REQUEST", 400);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
							
	}
	
	public void addRelationship(HttpExchange request) throws IOException {
		String query = Utils.convert(request.getRequestBody());
		System.out.println(query);
		
		try (Session session = driver.session()) {
			JSONObject jsonObj = new JSONObject(query);
			String actorId = jsonObj.getString("actorId");
			String movieId = jsonObj.getString("movieId");
			
			if (jsonObj.has("actorId") 
					&& jsonObj.has("movieId") 
					&& containsActorId(actorId) 
					&& containsMovieId(movieId)
					&& !existsRelationship(actorId, movieId)) {
				
				String cmd = "MATCH (a:actor),(m:movie)" + "\n" + 
						"WHERE a.actorId = $x AND m.movieId = $y" + "\n" + 
						"CREATE (a)-[r:ACTED_IN]->(m)" + "\n" + 
						"RETURN r";
				session.writeTransaction(tx -> tx.run(cmd, parameters("x", actorId, "y", movieId)));
				sendString(request, "OK", 200);			
			}
			else if (!containsActorId(actorId) 
					|| !containsMovieId(movieId)) 
				sendString(request, "NOT FOUND", 404);	
			else sendString(request, "BAD REQUEST", 400); 
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
					
	}
	
	public void getActor(HttpExchange request) throws IOException {
		URI uri = request.getRequestURI();
		String query = uri.getQuery();
		String actorId = Utils.splitQuery(query).get("actorId");
		
		try {		
			if (actorId != null 
					&& containsActorId(actorId)) {
				int begin, end;
				StatementResult result;
				String name = null;
				List<String> movies = new ArrayList<>();
				
				try (Session session = driver.session()) {
					String cmd =  "MATCH(a:actor {actorId: $x}) RETURN a.name";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", actorId)));
					name = result.list().get(0).toString();
					begin = (name.toString()).indexOf('"') + 1;
					end = (name.toString()).indexOf('"', begin);
					name = name.toString().substring(begin, end);
					//System.out.println(name);
					
				}
				catch (Exception e){
					
				}
				
				try (Session session = driver.session()) {
					
					String cmd =  "MATCH(a:actor {actorId: $x})-[:ACTED_IN*1]->(res) RETURN res.movieId";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", actorId)));
					List<Record> records = result.list();
					
					for (Record r : records) {
						begin = (r.toString()).indexOf('"') + 1;
						end = (r.toString()).indexOf('"', begin);
						movies.add(r.toString().substring(begin, end));
					}
					
				}
				catch (Exception e){
					
				}
				
				try {
					
					JSONObject json = new JSONObject();
					json.put("actorId", actorId);
					json.put("name", name);
					json.put("movies", movies);
					System.out.print(json.toString());
					sendString(request, json.toString(), 200);
					
				}
				catch (Exception e){
					
				}
			}
			else if (!containsActorId(actorId)) sendString(request, "NOT FOUND", 404);
			else {
				sendString(request, "BAD REQUEST", 400);			
			}
		}
		catch (Exception e) {
				
		}
				
				/*
				
					if (name != null) {
						String json = "{\n\t\"actorId\" : " + Utils.inQuotes(actorId) + ",\n\t\"name\" : " + Utils.inQuotes(name) + ",\n\t\"movies\": [";
						if (!movies.isEmpty()) {
							json += "\n";
							for (String movie : movies) json += "\t\t" + Utils.inQuotes(movie) + ",\n";
							json += "\t";
						}
						
						json += "]\n}\n";
						
						JSONObject obj = new JSONObject(json);
						System.out.print(obj.toString());
						sendString(request, json, 200);
						
					}
					else sendString(request, "NOT FOUND", 404);
					
				} 
				catch (Exception e) {
					
				} */
		
					
	}
	
	public void getMovie(HttpExchange request) throws IOException {
		URI uri = request.getRequestURI();
		String query = uri.getQuery();
		String movieId = Utils.splitQuery(query).get("movieId");
		
		try {		
			if (movieId != null 
					&& containsMovieId(movieId)) {
				int begin, end;
				StatementResult result;
				String name = null;
				List<String> actors = new ArrayList<>();
				
				try (Session session = driver.session()) {
					String cmd =  "MATCH(m:movie {movieId: $x}) RETURN m.name";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", movieId)));
					name = result.list().get(0).toString();
					begin = (name.toString()).indexOf('"') + 1;
					end = (name.toString()).indexOf('"', begin);
					name = name.toString().substring(begin, end);
					//System.out.println(name);
					
				}
				catch (Exception e){
					
				}
				
				try (Session session = driver.session()) {
					
					String cmd =  "MATCH(m:movie {movieId: $x})-[:ACTED_IN*1]-(res) RETURN res.actorId";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", movieId)));
					List<Record> records = result.list();
					
					for (Record r : records) {
						begin = (r.toString()).indexOf('"') + 1;
						end = (r.toString()).indexOf('"', begin);
						actors.add(r.toString().substring(begin, end));
					}
					
				}
				catch (Exception e){
					
				}
				
				try {
					
					JSONObject json = new JSONObject();
					json.put("movieId", movieId);
					json.put("name", name);
					json.put("actors", actors);
					System.out.print(json.toString());
					sendString(request, json.toString(), 200);
					
				}
				catch (Exception e){
					
				}
			}
			else if (!containsMovieId(movieId)) sendString(request, "NOT FOUND", 404);
			else {
				sendString(request, "BAD REQUEST", 400);			
			}
		}
		catch (Exception e) {
				
		}
					
	}
	
	
	public void hasRelationship(HttpExchange request) throws Exception
    {
		String result;
        try (Session session = driver.session())
        {
        	URI uri = request.getRequestURI();
    		String query = uri.getQuery();
    		String actorId = Utils.splitQuery(query).get("actorId");
    		String movieId = Utils.splitQuery(query).get("movieId");
        	   	
        	try {
        		if (actorId != null 
        				&& movieId != null
        				&& containsActorId(actorId)
        				&& containsMovieId(movieId)) {
    				String cmd =  "RETURN EXISTS ((: actor {actorId: $x})-[:ACTED_IN*1]-(: movie {movieId: $y} ))"
		      				+ "AS bool\n";
    				StatementResult res = session.readTransaction(tx -> tx.run(cmd, parameters("x", actorId, "y", movieId)));
    				result = res.single().get("bool").toString();
    	        	if (result.equals("TRUE")) result = "true";
    	        	else result = "false";
    	        	JSONObject obj = new JSONObject();
    	        	obj.put("actorId", actorId);
    	        	obj.put("movieId", movieId);
    	        	obj.put("hasRelationship", result);
    				sendString(request, obj.toString(), 200);
    			}
        		else if (actorId == null 
        				|| movieId == null) sendString(request, "BAD REQUEST", 400);
        		else if (!containsActorId(actorId) 
        				|| !containsMovieId(movieId)) sendString(request, "NOT FOUND", 404);
    			else sendString(request, "BAD REQUEST", 400);
    		}
    		catch (Exception e) {
    			e.printStackTrace();
    		}

        }
    }
    
	
	public void computeBaconNumber(HttpExchange request) throws IOException{
		URI uri = request.getRequestURI();
		String query = uri.getQuery();
		String actorId = Utils.splitQuery(query).get("actorId");
		//System.out.print(actorId);
        try (Session session = driver.session()) {
            
        	if (actorId != null && containsActorId(actorId)) {
        		int baconNumber;
        		if (actorId.equals("nm000102")) baconNumber = 0;
        		else {
        			try (Transaction tx = session.beginTransaction()) {
                        String cmd = "MATCH (a:actor),(kb:actor),"
                        		+ "p = shortestPath((a)-[*..15]-(kb))"
                        		+ "WHERE a.actorId = $x AND kb.actorId = 'nm000102'"
                        		+ "RETURN length(p) as baconNumber";
                    	StatementResult result = tx.run(cmd, parameters("x", actorId));
                        baconNumber = result.single().get("baconNumber").asInt()/2;
        			}
        			
        		}
        		JSONObject json = new JSONObject();
        		json.put("baconNumber", baconNumber);
                sendString(request, json.toString(), 200);
        	}	
        	else if (!containsActorId(actorId)) sendString(request, "NOT FOUND", 404);
        	else sendString(request, "BAD REQUEST", 400);
             
        }
        catch (Exception e) {
        	
        }
    } 
	
	public void computeBaconPath(HttpExchange request) throws IOException {
		URI uri = request.getRequestURI();
		String query = uri.getQuery();
		String actorId = Utils.splitQuery(query).get("actorId");	
		try (Session session = driver.session()) {
			
			
			if (actorId != null && containsActorId(actorId)) {
				JSONObject json = new JSONObject();
				if (actorId.equals("nm000102")) {
					List<Object> baconPath = new ArrayList<>();
					baconPath.add("nm0000102");
					json.put("baconPath", baconPath);
				}
				else {
					JSONArray jlist;
					try (Transaction tx = session.beginTransaction()) {
						String cmd = "MATCH (a:actor),(kb:actor),"
								+ "p = shortestPath((a)-[*..15]-(kb))"
								+ "WHERE a.actorId = $x AND kb.actorId = 'nm000102'"
								+ "RETURN nodes(p) as path";
						StatementResult result = tx.run(cmd, parameters("x", actorId));
						List<String> baconPath = new ArrayList<>();

	                    if (result.hasNext()) {
	                        Record record = result.next();
	                        List<Object> path = record.get("path").asList();
	                        int i = 0;
	                        for (Object nodeValue : path) {
	                            Node node = (Node) nodeValue;
	                            String label = (i % 2 == 0) ? "actorId" : "movieId";                      	
	                            baconPath.add(Utils.removeQuotes(node.get(label).toString()));
	                            i++;
	                        }
	                    }
	                    
	                    jlist = new JSONArray(baconPath);
	                    json.put("baconPath", jlist);
					}
					
				}
				sendString(request, json.toString(), 200);
			}
			else if (!containsActorId(actorId)) sendString(request, "NOT FOUND", 404);
			else sendString(request, "BAD REQUEST", 400);
		}	
		catch (Exception e) {
		}
	
    }

	
	private boolean containsActorId(String actorId) throws Exception
    {
        try (Session session = driver.session())
        {
        	String result; 
        	
        	try (Transaction tx = session.beginTransaction()) {
        		StatementResult res = tx.run("OPTIONAL MATCH (a:actor {actorId: $x}) RETURN a IS NOT NULL AS predicate", parameters("x", actorId) );
        		result = res.single().get("predicate").toString();
        		//System.out.print(result);
        		
        	}
        	
        	if (result.equals("TRUE")) return true;
    		else return false;
        }
    }
	
	private boolean containsMovieId(String movieId) throws Exception
    {
        try (Session session = driver.session())
        {
        	String result; 
        	
        	try (Transaction tx = session.beginTransaction()) {
        		StatementResult res = tx.run("OPTIONAL MATCH (m:movie {movieId: $x}) RETURN m IS NOT NULL AS predicate", parameters("x", movieId) );
        		result = res.single().get("predicate").toString();
        		//System.out.print(result);      		
        	}
        	
        	if (result.equals("TRUE")) return true;
    		else return false;
        }
    }
	
	private boolean existsRelationship(String actorId, String movieId) throws Exception
    {
        try (Session session = driver.session())
        {
        	String result; 
        	
        	try (Transaction tx = session.beginTransaction()) {
        		StatementResult res = tx.run("RETURN EXISTS ((: actor {actorId: $x})-[:ACTED_IN*1]-(: movie {movieId: $y} ))"
        				      				+ "AS bool\n", parameters("x", actorId, "y", movieId) );
        		result = res.single().get("bool").toString();
        		//System.out.print(result);      		
        	}
        	
        	if (result.equals("TRUE")) return true;
    		else return false;
        }
    }
	
	public void close() {
		driver.close();
	}
	
	private void sendString(HttpExchange request, String data, int restCode) throws IOException {
		request.sendResponseHeaders(restCode, data.length());
        OutputStream os = request.getResponseBody();
        os.write(data.getBytes());
        os.close();
	}

}
