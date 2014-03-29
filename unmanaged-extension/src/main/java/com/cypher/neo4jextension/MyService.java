package com.cypher.neo4jextension;

import org.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.algolia.search.saas.*;




@Path("/import_helper")
public class MyService {
    APIClient client = new APIClient("A0EF2HAQR0", "906b9d732ffa5e9e32bbf7e6008ba6d3");
    private static Logger logger = LoggerFactory.getLogger(MyService.class);
    String env = "test";

    public enum OrgRelationshipTypes implements RelationshipType
    {
        has_parent_organization, has_position, has_executive_position, has_provider, has_owner, has_acquired,
        was_acquired, has_competitor, has_address, has_headquarters, has_external_service, awarded_degree,
        has_funding_round, has_fascilitator, organization_in_market, has_recipient, has_organizer, has_speaker,
        has_sponsor, has_exhibitor, has_primary_affiliation, has_membership, has_mention, has_partner, has_customer,
        invested_in, has_founder, has_notable_graduate, has_advisory_role_position, went_public, sponsors_event,
        has_facilitator, has_lead_investor, has_product, public_on, has_web_reference, has_raised_fund, has_invested,
        sponsors_award, has_primary_image
    }

    public enum PersonRelationshipTypes implements RelationshipType
    {
        has_owner, has_new_owner, has_previous_owner, has_degree, has_web_reference, has_recipient,
        has_job, has_primary_location, has_primary_affiliation, has_fascilitator, invested_in, has_mention,
        has_membership, has_advisory_role, has_speaker, has_facilitator, has_lead_investor, has_lead_partner,
        has_invested, has_organizer, has_sponsor, has_founder, has_notable_graduate, has_primary_image
    }

    public enum JobRelationshipTypes implements RelationshipType
    {
        has_job, has_position, has_executive_position, worked_on, has_advisory_role, has_advisory_role_position,
        has_primary_affiliation
    }

    public enum ProductRelationshipTypes implements RelationshipType
    {
        has_owner, has_competitor, has_customer, product_in_market, has_closure, worked_on, has_mention, has_launch,
        has_primary_affiliation, has_product,has_recipient, has_primary_image
    }

    public enum LocationRelationshipTypes implements RelationshipType
    {
        has_parent_location, has_location, has_primary_location
    }


    public String getFullLocationName(Node location){
        return fullLocationName(location, null);
    }

    public String fullLocationName(Node location, String accumulated_name){
        if (!location.hasRelationship(LocationRelationshipTypes.has_parent_location, Direction.OUTGOING) || location.getProperty("location_type") == "continent"){
            return accumulated_name;
        }
        Node parent_location = location.getSingleRelationship(LocationRelationshipTypes.has_parent_location, Direction.OUTGOING).getEndNode();
        if (accumulated_name == null){
            accumulated_name = (String) location.getProperty("name");
        } else {
            if (location.getProperty("location_type").equals("region")){
                if (location.hasProperty("location_code_2")){
                    accumulated_name += ", " + location.getProperty("location_code_2");
                } else {
                    accumulated_name += ", " + location.getProperty("name");
                }
            } else {
                if (location.hasProperty("location_code_3")){
                    accumulated_name += ", " + location.getProperty("location_code_3");
                } else {
                    accumulated_name += ", " + location.getProperty("name");
                }
            }
        }

        accumulated_name = fullLocationName(parent_location, accumulated_name);
        // should never arrive here (recursive)
        return accumulated_name;
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
        for ( Relationship r : GlobalGraphOperations.at( db ).getAllRelationships() ) {
            r.getPropertyKeys();
            start = r.getStartNode();
        }
        return "Warmed up and ready to go!";
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
            String market_names_flat = null, location_name = null;
            Integer n_relationships = 0;

            // get the market names for the organization if it exists
            if (org.hasRelationship(OrgRelationshipTypes.organization_in_market)){
                Iterable<Relationship> markets  = org.getRelationships(OrgRelationshipTypes.organization_in_market, Direction.BOTH);
                market_names_flat = "";
                while (markets.iterator().hasNext()) {
                    market_names_flat += markets.iterator().next().getEndNode().getProperty("name");
                    if (markets.iterator().hasNext()) market_names_flat += ",";
                }
            }

            // get the location for the organization if it exists
            if (org.hasRelationship(OrgRelationshipTypes.has_headquarters)){
                Iterable<Relationship> headquarters = org.getRelationships(OrgRelationshipTypes.has_headquarters);
                Node address = headquarters.iterator().next().getEndNode();
                if (address.hasRelationship(LocationRelationshipTypes.has_location)){
                    Node location = address.getSingleRelationship(LocationRelationshipTypes.has_location, Direction.OUTGOING).getEndNode();
                    location_name = getFullLocationName(location);
                }
            }

            // get the primary image for the organization if it exists
            Object image_id = null;
            if (org.hasRelationship(OrgRelationshipTypes.has_primary_image)){
                Relationship image_rel = org.getSingleRelationship(OrgRelationshipTypes.has_primary_image, Direction.BOTH);
                image_id = image_rel.getEndNode().getProperty("public_id");
            }

            for (Relationship r : org.getRelationships()) n_relationships++;

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
                algolia_obj.put("location", location_name);
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

    @GET
         @Path("/seed_algolia_people")
         public String seedAlgoliaPeople(@Context GraphDatabaseService db) {
        logger.info("Total Memory: " +  Runtime.getRuntime().totalMemory());
        logger.info("Max Memory: " +  Runtime.getRuntime().maxMemory());
        logger.info("Available Memory: " +  Runtime.getRuntime().freeMemory());
        logger.info("Available CPUs: " +  Runtime.getRuntime().availableProcessors());
        ReadableIndex<Node> nodeAutoIndex = db.index().getNodeAutoIndexer().getAutoIndex();
        Index main_index            = client.initIndex("main_" + env);
        Index person_index          = client.initIndex("person_" + env);
        Index event_organizer_index = client.initIndex("event_organizer_" + env);
        Index investor_index        = client.initIndex("investor_" + env);
        List<JSONObject> algolia_objects           = new LinkedList<JSONObject>();
        List<JSONObject> algolia_investor_objects  = new LinkedList<JSONObject>();
        List<JSONObject> algolia_event_org_objects = new LinkedList<JSONObject>();
        String cloudinary_url = "http://res.cloudinary.com/crunchbase" + env + "/image/upload/";
        for ( Node person : nodeAutoIndex.get("type", "Person")){
            Integer n_relationships = 0;

            // get the number of total relationships for the node
            for (Relationship r : person.getRelationships()) n_relationships++;

            // get the job title and the organization name if they exist
            Object job_title = null;
            Object org_name = null;
            if (person.hasRelationship(PersonRelationshipTypes.has_primary_affiliation)){
                Iterable<Relationship> primary_affiliation = person.getRelationships(PersonRelationshipTypes.has_primary_affiliation, Direction.OUTGOING);
                Node job = primary_affiliation.iterator().next().getEndNode();
                job_title = job.getProperty("title");
                if (job.hasRelationship(JobRelationshipTypes.has_position)){
                    Relationship has_position = job.getSingleRelationship(JobRelationshipTypes.has_position, Direction.INCOMING);
                    Node org = has_position.getStartNode();
                    org_name = org.getProperty("name");
                }
            }

            // get the primary location if it exists
            Object location_name = null;
            if (person.hasRelationship(PersonRelationshipTypes.has_primary_location)){
                Relationship primary_location = person.getSingleRelationship(PersonRelationshipTypes.has_primary_location, Direction.OUTGOING);
                Node location = primary_location.getEndNode();
                location_name = getFullLocationName(location);
            }


            // get the primary image if it exists
            Object image_id = null;
            if (person.hasRelationship(PersonRelationshipTypes.has_primary_image)) {
                Relationship image_rel = person.getSingleRelationship(PersonRelationshipTypes.has_primary_image, Direction.BOTH);
                image_id = image_rel.getEndNode().getProperty("public_id");
            }

            JSONObject algolia_obj = new JSONObject();
            try {
                algolia_obj.put("objectID", person.getProperty("uuid"));
                algolia_obj.put("type", person.getProperty("type"));
                algolia_obj.put("name", person.getProperty("first_name") + " " + person.getProperty("last_name"));
                algolia_obj.put("description", person.getProperty("short_description", null));
                algolia_obj.put("n_relationships", n_relationships);
                if (image_id != null) {
                    algolia_obj.put("logo_url", cloudinary_url + image_id);
                    algolia_obj.put("logo_url_30_30",cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_60_60", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_100_100", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_120_120", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                }
                if (job_title != null) algolia_obj.put("title", job_title);
                if (org_name != null) algolia_obj.put("organization_name", org_name);
                if (location_name != null) algolia_obj.put("location", location_name);

            } catch (Exception e) {
                e.printStackTrace();
            }

            algolia_objects.add(algolia_obj);
            if (person.hasProperty("role_investor")) algolia_investor_objects.add(algolia_obj);
            if (person.hasRelationship(PersonRelationshipTypes.has_organizer)) algolia_event_org_objects.add(algolia_obj);

            if (algolia_objects.size() == 1000) {
                logger.info("Available Memory: " +  Runtime.getRuntime().freeMemory());
            }

            if (algolia_objects.size() == 10000 ) {
                try {
                    main_index.saveObjects(algolia_objects);
                    person_index.saveObjects(algolia_objects);
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
            person_index.saveObjects(algolia_objects);
            event_organizer_index.saveObjects(algolia_event_org_objects);
            investor_index.saveObjects(algolia_investor_objects);
        } catch (AlgoliaException e){
            e.printStackTrace();
        }

        return "Seeded Algolia People";
    }

    @GET
    @Path("/seed_algolia_products")
    public String seedAlgoliaProducts(@Context GraphDatabaseService db) {
        logger.info("Total Memory: " +  Runtime.getRuntime().totalMemory());
        logger.info("Max Memory: " +  Runtime.getRuntime().maxMemory());
        logger.info("Available Memory: " +  Runtime.getRuntime().freeMemory());
        logger.info("Available CPUs: " +  Runtime.getRuntime().availableProcessors());
        ReadableIndex<Node> nodeAutoIndex = db.index().getNodeAutoIndexer().getAutoIndex();
        Index main_index             = client.initIndex("main_" + env);
        Index product_index          = client.initIndex("product_" + env);
        List<JSONObject> algolia_objects           = new LinkedList<JSONObject>();
        String cloudinary_url = "http://res.cloudinary.com/crunchbase" + env + "/image/upload/";
        for ( Node product : nodeAutoIndex.get("type", "Product")){
            Integer n_relationships = 0;
            String market_names_flat = null;

            // get the number of total relationships for the node
            for (Relationship r : product.getRelationships()) n_relationships++;


            // get the market names for the organization if it exists
            if (product.hasRelationship(ProductRelationshipTypes.product_in_market)){
                Iterable<Relationship> markets  = product.getRelationships(ProductRelationshipTypes.product_in_market, Direction.BOTH);
                market_names_flat = "";
                while (markets.iterator().hasNext()) {
                    market_names_flat += markets.iterator().next().getEndNode().getProperty("name");
                    if (markets.iterator().hasNext()) market_names_flat += ",";
                }
            }

            // get the organization name if it exists
            Object organization_name = null;
            if (product.hasRelationship(ProductRelationshipTypes.has_product)){
                Relationship primary_location = product.getSingleRelationship(ProductRelationshipTypes.has_product, Direction.INCOMING);
                Node organization = primary_location.getStartNode();
                organization_name = organization.getProperty("name");
            }

            // get the primary image if it exists
            Object image_id = null;
            if (product.hasRelationship(ProductRelationshipTypes.has_primary_image)) {
                Relationship image_rel = product.getSingleRelationship(ProductRelationshipTypes.has_primary_image, Direction.BOTH);
                image_id = image_rel.getEndNode().getProperty("public_id");
            }

            JSONObject algolia_obj = new JSONObject();
            try {
                algolia_obj.put("objectID", product.getProperty("uuid"));
                algolia_obj.put("type", product.getProperty("type"));
                algolia_obj.put("name", product.getProperty("name"));
                algolia_obj.put("description", product.getProperty("short_description", null));
                algolia_obj.put("n_relationships", n_relationships);
                if (image_id != null) {
                    algolia_obj.put("logo_url", cloudinary_url + image_id);
                    algolia_obj.put("logo_url_30_30",cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_60_60", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_100_100", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                    algolia_obj.put("logo_url_120_120", cloudinary_url + "w_30,h_30,c_fill,g_face/" + image_id);
                }
                if (market_names_flat != null) algolia_obj.put("markets", market_names_flat);
                if (organization_name != null) algolia_obj.put("organization_name", organization_name);

            } catch (Exception e) {
                e.printStackTrace();
            }

            algolia_objects.add(algolia_obj);

            if (algolia_objects.size() == 1000) {
                logger.info("Available Memory: " +  Runtime.getRuntime().freeMemory());
            }

            if (algolia_objects.size() == 10000 ) {
                try {
                    main_index.saveObjects(algolia_objects);
                    product_index.saveObjects(algolia_objects);
                } catch (AlgoliaException e){
                    e.printStackTrace();
                }
                algolia_objects.clear();
            }
        }
        try {
            main_index.saveObjects(algolia_objects);
            product_index.saveObjects(algolia_objects);
        } catch (AlgoliaException e){
            e.printStackTrace();
        }

        return "Seeded Algolia Products";
    }

}