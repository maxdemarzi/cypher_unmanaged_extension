package com.cypher.neo4jextension;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MyServiceTest {

    private GraphDatabaseService db;
    private MyService service;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final RelationshipType FRIENDS = DynamicRelationshipType.withName("FRIENDS");

    @Before
    public void setUp() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        populateDb(db);
        service = new MyService();
    }

    private void populateDb(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        try
        {
            Node userA = createUsers(db, "A");
            Node userB = createUsers(db, "B");
            Node userC = createUsers(db, "C");
            Node userD = createUsers(db, "D");
            Node userE = createUsers(db, "E");
            Node userF = createUsers(db, "F");

            /*
              A-B-C
                B-D
                B-F
              A-E-C
              A-F
             */

            userA.createRelationshipTo(userB, FRIENDS);
            userA.createRelationshipTo(userE, FRIENDS);
            userA.createRelationshipTo(userF, FRIENDS);
            userB.createRelationshipTo(userC, FRIENDS);
            userE.createRelationshipTo(userC, FRIENDS);
            userB.createRelationshipTo(userD, FRIENDS);
            userB.createRelationshipTo(userF, FRIENDS);

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private Node createUsers(GraphDatabaseService db, String username) {
        Index<Node> users = db.index().forNodes("Users");
        Node node = db.createNode();
        node.setProperty("username", username);
        users.add(node, "username", username);
        return node;
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();

    }

}
