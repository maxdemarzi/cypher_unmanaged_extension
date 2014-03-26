package com.cypher.neo4jextension;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;
import scala.util.parsing.json.JSONObject;

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

        Index index = client.initIndex("organization_development");
        List<JSONObject> algolia_objects = new ArrayList<JSONObject>();
        for ( Node org : node_auto_index.get("type", "Organization")){
            String[] market_names = {};
            String market_names_flat, location = "";
            Integer n_relationships;
            Relationship[] markets;
            markets  = org.getRelationships("organization_in_market");
            location = org.getRelationships("has_headquarters").getEndNode.full_name;
            for ( Relationship market : markets ) {
                market_names.add( market.getEndNode.name );
            }

            for ( String str : market_names) market_names_flat = market_names_flat + ", " + str;

            n_relationships = Array.getLength(n.getRelationships);


            JSONObject[] algolia_obj;
            algolia_obj.put( "objectId", org.uuid );
            algolia_obj.put( "type", org.type );
            algolia_obj.put( "name", org.name );
            algolia_obj.put( "logo_url", org.image_id );
            algolia_obj.put( "markets", market_names_flat );
            algolia_obj.put( "description", org.description);
            algolia_obj.put( "primary_role", org.primary_role);
            algolia_obj.put( "location", location);
            algolia_obj.put( "homepage", org.homepage_url);
            algolia_obj.put( "n_relationships", n_relationships );

            algolia_objects.add(algolia_obj);

            if (algolia_objects.size() == 1000) {
                index.saveObjects(algolia_objects);
                algolia_objects.clear();
            }
            if (!algolia_objects.isEmpty()){
                index.saveObjects(algolia_objects);
            }
        }

        return "Seeded Algolia Organizations";
    }

}
