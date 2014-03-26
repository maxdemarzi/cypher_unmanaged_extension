package com.cypher.neo4jextension;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.algolia.search.saas.*;

@Path("/import_helper")
public class MyService {
    APIClient client = new APIClient("YourApplicationID", "YourAPIKey");

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
        for ( Relationship r : GlobalGraphOperations.at( db ).getAllRelationships() ) {
            r.getPropertyKeys();
            start = r.getStartNode();
        }
        return "Warmed up and ready to go!";
    }

    @GET
    @Path("/seed_algolia_orgs")
    public String seedAlgoliaOrgs(@Context GraphDatabaseService db) {

        ReadableIndex<Node> nodeAutoIndex = db.index().getNodeAutoIndexer().getAutoIndex();
        Index index = client.initIndex("organization_development");
        List<JSONObject> algolia_objects = new ArrayList<JSONObject>();
        for ( Node org : nodeAutoIndex.get("type", "Organization")){
            ArrayList<String> market_names = new ArrayList<String>();
            String market_names_flat = null, location = null;
            Integer n_relationships;
            Iterable<Relationship> markets  = org.getRelationships("organization_in_market");
            if (markets != null && markets.iterator().hasNext()) {
                market_names_flat = "";
                while (markets.iterator().hasNext()) {
                    market_names_flat += markets.iterator().next().getEndNode().getProperty('name');
                    if (markets.iterator().hasNext()) market_names_flat += ",";
                }
            }
            Iterable<Relationship> headquarters = org.getRelationships("has_headquarters");
            if (headquarters != null && headquarters.iterator().hasNext())
                location = (String) headquarters.iterator().next().getEndNode().getProperty("full_name");

            n_relationships = Array.getLength(n.getRelationships);

            JSONObject algolia_obj = new JSONObject();
            try {
                algolia_obj.put("objectId", org.getProperty("uuid"));
                algolia_obj.put("type", org.getProperty("type"));
                algolia_obj.put("name", org.getProperty("name"));
                algolia_obj.put("logo_url", org.getProperty("image_id"));
                algolia_obj.put("markets", market_names_flat);
                algolia_obj.put("description", org.getProperty("description"));
                algolia_obj.put("primary_role", org.getProperty("primary_role"));
                algolia_obj.put("location", location);
                algolia_obj.put("homepage", org.getProperty("homepage_url"));
                algolia_obj.put("n_relationships", n_relationships);
            } catch (Exception e) {
                e.printStackTrace();
            }
            algolia_objects.add(algolia_obj);
            if (algolia_objects.size() == 1000) {
                try {
                    index.saveObjects(algolia_objects);
                } catch (AlgoliaException e){
                    e.printStackTrace();
                }
                algolia_objects.clear();
            }
            if (!algolia_objects.isEmpty()){
                try {
                    index.saveObjects(algolia_objects);
                } catch (AlgoliaException e) {
                    e.printStackTrace();
                }
            }
        }

        return "Seeded Algolia Organizations";
    }

}
