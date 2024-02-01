package ca.yorku.eecs;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.*;

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
public class Database {
	
	private Driver driver;
	private String uriDb;
	
	public Database() {
		uriDb = "bolt://localhost:7687"; // may need to change if you used a different port for your DBMS
		Config config = Config.builder().withoutEncryption().build();
		driver = GraphDatabase.driver(uriDb, AuthTokens.basic("neo4j","12345678"), config);
	}
	
	public void addActor(String query, HttpExchange request) throws IOException {
		
		try {
			Session session = driver.session();
			String name = Utils.splitQuery(query).get("name");
			String actorID = Utils.splitQuery(query).get("actorId");
				
			if (name == null || actorID == null || containsActorID(actorID)) sendString(request, "BAD REQUEST", 400);
			else {
				String cypher =  "MERGE (a:actor {name: $x, actorID: $y})";
				session.writeTransaction(tx -> tx.run(cypher, parameters("x", name, "y", actorID)));
				sendString(request, "OK", 200);
				session.close();
			}
		}
		catch (Exception e) {
			
		}
		
					
	}
	
	public void addMovie(String query, HttpExchange request) throws IOException {
		
		try {
			Session session = driver.session();
			
			String name = Utils.splitQuery(query).get("name");
			String movieID = Utils.splitQuery(query).get("movieID");
			
			if (name == null || movieID == null || containsMovieID(movieID)) sendString(request, "BAD REQUEST", 400);
			else {
				String cypher =  "MERGE (m:movie {name: $x, movieID: $y})";
				session.writeTransaction(tx -> tx.run(cypher, parameters("x", name, "y", movieID)));
				sendString(request, "OK", 200);
				session.close();
			}
		}
		catch (Exception e) {
			
		}
		
					
	}
	
	public void addRelationship(String query, HttpExchange request) throws IOException {
		
		try {
			Session session = driver.session();
			String actorID = Utils.splitQuery(query).get("actorID");
			String movieID = Utils.splitQuery(query).get("movieID");
				
			if (actorID == null || movieID == null) sendString(request, "BAD REQUEST", 400);
			else if (!containsActorID(actorID) || !containsMovieID(movieID)) sendString(request, "NOT FOUND", 404);
			else {
				String cypher = "MATCH (a:actor),(m:movie)" + "\n" + 
						"WHERE a.actorID = $x AND m.movieID = $y" + "\n" + 
						"CREATE (a)-[r:ACTED_IN]->(m)" + "\n" + 
						"RETURN r";
				session.writeTransaction(tx -> tx.run(cypher, parameters("x", actorID, "y", movieID)));
				sendString(request, "OK", 200);
				session.close();
			}
		}
		catch (Exception e) {
			
		}
		
					
	}
	
	public void getActor(String query, HttpExchange request) throws IOException {
		
		String actorID = Utils.splitQuery(query).get("actorID");
		if (actorID == null) {
			sendString(request, "BAD REQUEST", 400);
		}
		else {
			try {
				
				
				int begin, end;
				StatementResult result;
				String name = null;
				List<String> movies = new ArrayList<>();
				
				try (Session session = driver.session()) {
					String cmd =  "MATCH(a:actor {actorID: $x}) RETURN a.name";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", actorID)));
					name = result.list().get(0).toString();
					begin = (name.toString()).indexOf('"') + 1;
					end = (name.toString()).indexOf('"', begin);
					name = name.toString().substring(begin, end);
					//System.out.println(name);
					
				}
				catch (Exception e){
					
				}
				
				try (Session session = driver.session()) {
					
					String cmd =  "MATCH(a:actor {actorID: $x})-[:ACTED_IN*1]->(res) RETURN res.movieID";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", actorID)));
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
					String json = "{\n\t\"actorID\" : " + Utils.inQuotes(actorID) + ",\n\t\"name\" : " + Utils.inQuotes(name) + ",\n\t\"movies\": [";
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
		
		String movieID = Utils.splitQuery(query).get("movieID");
		if (movieID == null) {
			sendString(request, "BAD REQUEST", 400);
		}
		else {
			try {
				
				
				int begin, end;
				StatementResult result;
				String name = null;
				List<String> actors = new ArrayList<>();
				
				try (Session session = driver.session()) {
					String cmd =  "MATCH(m:movie {movieID: $x}) RETURN m.name";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", movieID)));
					name = result.list().get(0).toString();
					begin = (name.toString()).indexOf('"') + 1;
					end = (name.toString()).indexOf('"', begin);
					name = name.toString().substring(begin, end);
					//System.out.println(name);
					
				}
				catch (Exception e){
					
				}
				
				try (Session session = driver.session()) {
					
					String cmd =  "MATCH(m:movie {movieID: $x})-[:ACTED_IN*1]-(res) RETURN res.actorID";
					result = session.readTransaction(tx -> tx.run(cmd, parameters("x", movieID)));
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
					String json = "{\n\t\"movieID\" : " + Utils.inQuotes(movieID) + ",\n\t\"name\" : " + Utils.inQuotes(name) + ",\n\t\"actors\": [";
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
	
	private boolean containsActorID(String actorID) throws Exception
    {
        try (Session session = driver.session())
        {
        	String result; 
        	
        	try (Transaction tx = session.beginTransaction()) {
        		StatementResult res = tx.run("OPTIONAL MATCH (a:actor {actorID: $x}) RETURN a IS NOT NULL AS predicate", parameters("x", actorID) );
        		result = res.single().get("predicate").toString();
        		//System.out.print(result);
        		
        	}
        	session.close();
        	
        	if (result.equals("TRUE")) return true;
    		else return false;
        }
    }
	
	private boolean containsMovieID(String movieID) throws Exception
    {
        try (Session session = driver.session())
        {
        	String result; 
        	
        	try (Transaction tx = session.beginTransaction()) {
        		StatementResult res = tx.run("OPTIONAL MATCH (m:movie {movieID: $x}) RETURN m IS NOT NULL AS predicate", parameters("x", movieID) );
        		result = res.single().get("predicate").toString();
        		//System.out.print(result);
        		
        	}
        	session.close();
        	
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

	private  void computeBaconNumber (String query, HttpExchange request) throws IOException{
		try {
			Session session = driver.session();
			String actorID = splitQuery(query).get("actorID");
			 if (actorID == null) sendString(request, "BAD REQUEST", 400);
	            else if (!containsActorID(actorID)) sendString(request, "NOT FOUND", 404);
	            else {
	                try (Transaction tx = session.beginTransaction()) {
	                    String cmd = "MATCH (a:Actor {actorID: $x}), (kb:Actor {actorID: 'nm0000102'}), p = shortestPath((a)-[:ACTED_IN*]-(kb)) RETURN length(p) AS baconNumber";
	                    StatementResult result = tx.run(cmd, parameters("x", actorID));
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
	        String actorID = splitQuery(query).get("actorID");

	        if (actorID == null) sendString(request, "BAD REQUEST", 400);
	        else if (!containsActorID(actorID)) sendString(request, "NOT FOUND", 404);
	        else {
	            try (Transaction tx = session.beginTransaction()) {
	                String cmd = "MATCH (a:Actor {actorID: $x}), (kb:Actor {actorID: 'nm0000102'}), p = shortestPath((a)-[:ACTED_IN*]-(kb)) RETURN nodes(p) AS path";
	                StatementResult result = tx.run(cmd, parameters("x", actorID));
	                List<String> baconPath = new ArrayList<>();

	                if (result.hasNext()) {
	                    Record record = result.next();
	                    List<Object> path = record.get("path").asList();
	                    for (Object nodeValue : path) {
	                        Value value = (Value) nodeValue;
	                        String actor = value.get("actorID").asString();
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
}

