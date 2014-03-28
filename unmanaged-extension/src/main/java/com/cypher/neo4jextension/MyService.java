package com.cypher.neo4jextension;

import org.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.lang.reflect.Array;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.algolia.search.saas.*;




@Path("/import_helper")
public class MyService {
    APIClient client = new APIClient("A0EF2HAQR0", "906b9d732ffa5e9e32bbf7e6008ba6d3");
    private static Logger logger = LoggerFactory.getLogger(MyService.class);
    String env = "test";

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

    public enum OrgRelationshipTypes implements RelationshipType
    {
        organization_in_market, has_headquarters, has_primary_image, has_organizer
    }

    @GET
    @Path("/seed_algolia_orgs")
    public String seedAlgoliaOrgs(@Context GraphDatabaseService db) {
        logger.info("Total Memory: " +  Runtime.getRuntime().totalMemory());
        logger.info("Max Memory: " +  Runtime.getRuntime().maxMemory());
        logger.info("Available Memory: " +  Runtime.getRuntime().freeMemory());
        logger.info("Available CPUs: " +  Runtime.getRuntime().availableProcessors());
        ReadableIndex<Node> nodeAutoIndex = db.index().getNodeAutoIndexer().getAutoIndex();
        Index main_index            = client.initIndex("main_" + env);
        Index organization_index    = client.initIndex("organization_" + env);
        Index event_organizer_index = client.initIndex("event_organizer_" + env);
        Index investor_index        = client.initIndex("investor_" + env);
        List<JSONObject> algolia_objects           = new LinkedList<JSONObject>();
        List<JSONObject> algolia_investor_objects  = new LinkedList<JSONObject>();
        List<JSONObject> algolia_event_org_objects = new LinkedList<JSONObject>();
        String cloudinary_url = "http://res.cloudinary.com/crunchbase" + env + "/image/upload/";
        for ( Node org : nodeAutoIndex.get("type", "Organization")){
            String market_names_flat = null, location = null;
            Integer n_relationships = 0;
            Iterable<Relationship> markets  = org.getRelationships(OrgRelationshipTypes.organization_in_market, Direction.BOTH);
            if (markets != null && markets.iterator().hasNext()) {
                market_names_flat = "";
                while (markets.iterator().hasNext()) {
                    market_names_flat += markets.iterator().next().getEndNode().getProperty("name");
                    if (markets.iterator().hasNext()) market_names_flat += ",";
                }
            }
            Iterable<Relationship> headquarters = org.getRelationships(OrgRelationshipTypes.has_headquarters);
            if (headquarters != null && headquarters.iterator().hasNext())
                location = (String) headquarters.iterator().next().getEndNode().getProperty("city", null);

            for (Relationship r : org.getRelationships()) n_relationships++;

            Relationship image_rel = org.getSingleRelationship(OrgRelationshipTypes.has_primary_image, Direction.BOTH);
            Object image_id = null;
            if (image_rel != null) {
                image_id = image_rel.getEndNode().getProperty("public_id");
            }



            JSONObject algolia_obj = new JSONObject();
            try {
                algolia_obj.put("objectID", org.getProperty("uuid"));
                algolia_obj.put("type", org.getProperty("type"));
                algolia_obj.put("name", org.getProperty("name"));
                if (image_id != null) {
                    algolia_obj.put("logo_url", cloudinary_url + image_id);
                    algolia_obj.put("logo_url_30_30",cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_60_60", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_100_100", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_120_120", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                }
                algolia_obj.put("markets", market_names_flat);
                algolia_obj.put("description", org.getProperty("short_description", null));
                algolia_obj.put("primary_role", org.getProperty("primary_role"));
                algolia_obj.put("location", location);
                //algolia_obj.put("homepage", org.getProperty("homepage_url"));
                algolia_obj.put("n_relationships", n_relationships);
            } catch (Exception e) {
                e.printStackTrace();
            }

            algolia_objects.add(algolia_obj);
            if (org.hasProperty("role_investor")) algolia_investor_objects.add(algolia_obj);
            if (org.hasRelationship(OrgRelationshipTypes.has_organizer)) algolia_event_org_objects.add(algolia_obj);

            if (algolia_objects.size() == 1000) {
                logger.info("Available Memory: " +  Runtime.getRuntime().freeMemory());
            }

            if (algolia_objects.size() == 10000 ) {
                //System.out.println("We are about to batch 1000");
                try {
                    main_index.saveObjects(algolia_objects);
                    organization_index.saveObjects(algolia_objects);
                    event_organizer_index.saveObjects(algolia_event_org_objects);
                    investor_index.saveObjects(algolia_investor_objects);
                } catch (AlgoliaException e){
                    e.printStackTrace();
                }
                algolia_objects.clear();
                algolia_event_org_objects.clear();
                algolia_investor_objects.clear();
            }
        }
        try {
            main_index.saveObjects(algolia_objects);
            organization_index.saveObjects(algolia_objects);
            event_organizer_index.saveObjects(algolia_event_org_objects);
            investor_index.saveObjects(algolia_investor_objects);
        } catch (AlgoliaException e){
            e.printStackTrace();
        }

        return "Seeded Algolia Organizations";
    }

}