package com.cypher.neo4jextension;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

@Path("/service")
public class MyService {

    ObjectMapper objectMapper = new ObjectMapper();
    private static final RelationshipType FRIENDS = DynamicRelationshipType.withName("FRIENDS");

    @GET
    @Path("/helloworld")
    public String helloWorld() {
        return "Hello World!";
    }

    @GET
    @Path("/warmup")
    public String warmUp(@Context GraphDatabaseService db) {
        Node start;
        for ( Node n : GlobalGraphOperations.at( db ).getAllNodes() ) {
            n.getPropertyKeys();
            for ( Relationship relationship : n.getRelationships() ) {
                start = relationship.getStartNode();
            }
        }
        for ( Relationship r : GlobalGraphOperations.at(db).getAllRelationships() ) {
            r.getPropertyKeys();
            start = r.getStartNode();
        }
        return "Warmed up and ready to go!";
    }

    @GET
    @Path("/friends/{username}")
    public Response getFriends(@PathParam("username") String username, @Context GraphDatabaseService db) throws IOException {
        List<String> results = new ArrayList<String>();
        IndexHits<Node> users = db.index().forNodes("Users").get("username", username);
        Node user = users.getSingle();

        for ( Relationship relationship : user.getRelationships(FRIENDS, Direction.OUTGOING) ){
            Node friend = relationship.getEndNode();
            results.add((String)friend.getProperty("username"));
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/fofs/{username}")
    public Response getFofs(@PathParam("username") String username, @Context GraphDatabaseService db) throws IOException {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();

        HashMap<Node, AtomicInteger> fofs = new HashMap<Node, AtomicInteger>();

        IndexHits<Node> users = db.index().forNodes("Users").get("username", username);
        Node user = users.getSingle();

        findFofs(fofs, user);
        List<Entry<Node, AtomicInteger>> fofList = orderFofs(fofs);
        returnFofs(results, fofList.subList(0, Math.min(fofList.size(), 10)));

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    private void findFofs(HashMap<Node, AtomicInteger> fofs, Node user) {
        List<Node> friends = new ArrayList<Node>();

        if (user != null){
            for ( Relationship relationship : user.getRelationships(FRIENDS, Direction.BOTH) ){
                Node friend = relationship.getOtherNode(user);
                friends.add(friend);
            }

            for ( Node friend : friends ){
                for (Relationship otherRelationship : friend.getRelationships(FRIENDS, Direction.BOTH) ){
                    Node fof = otherRelationship.getOtherNode(friend);
                    if (!user.equals(fof) && !friends.contains(fof)) {
                        AtomicInteger atomicInteger = fofs.get(fof);
                        if (atomicInteger == null) {
                            fofs.put(fof, new AtomicInteger(1));
                        } else {
                            atomicInteger.incrementAndGet();
                        }
                    }
                }
            }
        }
    }
    private List<Entry<Node, AtomicInteger>> orderFofs(HashMap<Node, AtomicInteger> fofs) {
        List<Entry<Node, AtomicInteger>> fofList = new ArrayList<Entry<Node, AtomicInteger>>(fofs.entrySet());
        Collections.sort(fofList, new Comparator<Entry<Node, AtomicInteger>>() {
            @Override
            public int compare(Entry<Node, AtomicInteger> a,
                               Entry<Node, AtomicInteger> b) {
                return ( b.getValue().get() - a.getValue().get() );

            }
        });
        return fofList;
    }

    private void returnFofs(List<Map<String, Object>> results, List<Entry<Node, AtomicInteger>> fofList) {
        Map<String, Object> resultsEntry;
        Map<String, Object> fofEntry;
        Node fof;
        for (Entry<Node, AtomicInteger> entry : fofList) {
            resultsEntry = new HashMap<String, Object>();
            fofEntry = new HashMap<String, Object>();
            fof = entry.getKey();

            for (String prop : fof.getPropertyKeys()) {
                fofEntry.put(prop, fof.getProperty(prop));
            }

            resultsEntry.put("fof", fofEntry);
            resultsEntry.put("friend_count", entry.getValue());
            results.add(resultsEntry);
        }
    }
}
