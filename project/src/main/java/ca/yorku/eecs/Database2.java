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
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import org.json.*;

import com.sun.net.httpserver.HttpExchange;

@SuppressWarnings("restriction")
public class Database2 {
	
	private Driver driver;
	private String uriDb;
	
	public Database2() {
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

			if (jsonObj.has("name")&&jsonObj.has("actorId") && !containsActorId(actorId)) {
				String cmd =  "MERGE (a:actor {name: $x, actorId: $y})";
				session.writeTransaction(tx -> tx.run(cmd, parameters("x", name, "y", actorId)));
				sendString(request, "OK", 200);
			}
			else sendString(request, "BAD REQUEST", 400);
			
			/*
			if (name == null || actorId == null || containsactorId(actorId)) sendString(request, "BAD REQUEST", 400);
			else {
				String cmd =  "MERGE (a:actor {name: $x, actorId: $y})";
				session.writeTransaction(tx -> tx.run(cmd, parameters("x", name, "y", actorId)));
				sendString(request, "OK", 200);		
			}*/
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
			
			if (jsonObj.has("name")&&jsonObj.has("movieId") && !containsMovieId(movieId)) {
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
			String actorId = Utils.splitJSON(query).get("actorId");
			String movieId = Utils.splitJSON(query).get("movieId");
				
			if (actorId == null || movieId == null || existsRelationship(actorId, movieId)) sendString(request, "BAD REQUEST", 400);
			else if (!containsActorId(actorId) || !containsMovieId(movieId)) sendString(request, "NOT FOUND", 404);
			else {
				String cmd = "MATCH (a:actor),(m:movie)" + "\n" + 
						"WHERE a.actorId = $x AND m.movieId = $y" + "\n" + 
						"CREATE (a)-[r:ACTED_IN]->(m)" + "\n" + 
						"RETURN r";
				session.writeTransaction(tx -> tx.run(cmd, parameters("x", actorId, "y", movieId)));
				sendString(request, "OK", 200);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
					
	}
	
	public void getActor(String query, HttpExchange request) throws IOException {
		
		String actorId = Utils.splitQuery(query).get("actorId");
		if (actorId == null) {
			sendString(request, "BAD REQUEST", 400);
		}
		else {
			try {
				
				
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
				
			}
		}
		
		
					
	}
	
	public void getMovie(String query, HttpExchange request) throws IOException {
		
		String movieId = Utils.splitQuery(query).get("movieId");
		if (movieId == null) {
			sendString(request, "BAD REQUEST", 400);
		}
		else {
			try {
				
				
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
				
				if (name != null) {
					String json = "{\n\t\"movieId\" : " + Utils.inQuotes(movieId) + ",\n\t\"name\" : " + Utils.inQuotes(name) + ",\n\t\"actors\": [";
					if (!actors.isEmpty()) {
						json += "\n";
						for (String actor : actors) json += "\t\t" + Utils.inQuotes(actor) + ",\n";
						json += "\t";
					}
					
					json += "]\n}\n";
					
					JSONObject obj = new JSONObject(json);
					sendString(request, json, 200);
					
				}
				else sendString(request, "NOT FOUND", 404);
				
			}
			catch (Exception e) {
				
			}
		}
		
		
					
	}
	
	private  void computeBaconNumber (String query, HttpExchange request) throws IOException{
        try {
            Session session = driver.session();
            String actorId = Utils.splitQuery(query).get("actorId");
             if (actorId == null) sendString(request, "BAD REQUEST", 400);
                else if (!containsActorId(actorId)) sendString(request, "NOT FOUND", 404);
                else {
                    try (Transaction tx = session.beginTransaction()) {
                        String cmd = "MATCH (a:actor {actorId: $x}), (kb:actor {actorId: 'nm0000102'}), p = shortestPath((a)-[:ACTED_IN*]-(kb)) RETURN length(p) AS baconNumber";
                        StatementResult result = tx.run(cmd, parameters("x", actorId));
                        int baconNumber = result.single().get("baconNumber").asInt();

                        sendString(request, String.valueOf(baconNumber), 200);
                    } catch (Exception e) {

                    }

                    session.close();
                }
            } catch (Exception e) {

        }
    } 
	
	public void computeBaconPath(String query, HttpExchange request) throws IOException {
        try {
            Session session = driver.session();
            String actorId = Utils.splitQuery(query).get("actorId");

            if (actorId == null) sendString(request, "BAD REQUEST", 400);
            else if (!containsActorId(actorId)) sendString(request, "NOT FOUND", 404);
            else {
                try (Transaction tx = session.beginTransaction()) {
                    String cmd = "MATCH (a:actor {actorId: $x}), (kb:actor {actorId: 'nm0000102'}), p = shortestPath((a)-[:ACTED_IN*]-(kb)) RETURN nodes(p) AS path";
                    StatementResult result = tx.run(cmd, parameters("x", actorId));
                    List<String> baconPath = new ArrayList<>();

                    if (result.hasNext()) {
                        Record record = result.next();
                        List<Object> path = record.get("path").asList();
                        for (Object nodeValue : path) {
                            Value value = (Value) nodeValue;
                            String actor = value.get("actorId").asString();
                            baconPath.add(actor);
                        }
                    }
                    sendString(request, String.join(",", baconPath), 200);
                } catch (Exception e) {
                }

                session.close();
            }
        } catch (Exception e) {
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
        		StatementResult res = tx.run("RETURN\n"
        				+ "EXISTS (( a: actor {actorID: $x})-[:ACTED_IN*1]-(m: movie {movieId: $y} ))\n"
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
