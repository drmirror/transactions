package net.drmirror.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Date;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.client.model.ValidationOptions;

import net.drmirror.transaction.Transaction.Status;



public class TransactionTest {

    private String CONNECTION = "localhost:27017";
    private String DATABASE = "transtest";
    private String TRANSACTION_COLLECTION = "transactions";
    private String DATA_COLLECTION = "data";
    
    private MongoClient client = null;
    
    private MongoClient client() {
        if (client == null) client = new MongoClient(CONNECTION);
        return client;
    }
    
    private MongoCollection<Document> transactionCollection = null;
    
    private MongoCollection<Document> transactionCollection() {
        if (transactionCollection == null)
            transactionCollection = client().getDatabase(DATABASE).getCollection(TRANSACTION_COLLECTION);
        return transactionCollection;
    }

    private MongoCollection<Document> dataCollection = null;
    
    private MongoCollection<Document> dataCollection() {
        if (dataCollection == null)
            dataCollection = client().getDatabase(DATABASE).getCollection(DATA_COLLECTION);
        return dataCollection;
    }

    private class TestTransaction extends Transaction {
        public TestTransaction(MongoClient client, Object id1, Object id2, int delta) {
            super(client, DATABASE, TRANSACTION_COLLECTION);
            addDocument (DATABASE, DATA_COLLECTION, id1);
            addDocument (DATABASE, DATA_COLLECTION, id2);
            transaction.append("delta", delta);
            saveTransaction(Status.INITIAL);
        }

        public void apply(Document... d) {
            int v0 = d[0].getInteger("value");
            int v1 = d[1].getInteger("value");
            int delta = transaction.getInteger("delta",0);
            v0 -= delta;
            v1 += delta;
            d[0].put("value", v0);
            data.get(0).setModified(true);
            d[1].put("value", v1);
            data.get(1).setModified(true);
        }
    }
    
    private void assertValue (int _id, Object value) {
        Document d = dataCollection().find(Filters.eq("_id", _id)).first();
        assertEquals(value, d.get("value"));
        assertFalse(d.containsKey("lock"));
    }
    
    private void assertStatus (Transaction t, Transaction.Status s) {
        Document d = transactionCollection().find(Filters.eq("_id", t.getId())).first();
        assertEquals(s.toString().toLowerCase(), d.getString("status"));
    }
    
    @Test
    public void testNormalExecution() {
        Document a = new Document("_id", 1).append("value", 100);
        Document b = new Document("_id", 2).append("value", 50);
        dataCollection().drop();
        dataCollection().insertOne(a);
        dataCollection().insertOne(b);
        Transaction t = new TestTransaction(client(), a.get("_id"), b.get("_id"), 10);
        t.execute();
        assertValue(1, 90);
        assertValue(2, 60);
        assertStatus(t,Status.COMPLETE);
        
        t = new TestTransaction(client(), a.get("_id"), b.get("_id"), 10);
        t.execute();
        assertValue(1, 80);
        assertValue(2, 70);
        assertStatus(t,Status.COMPLETE);
    }
    
    @Test
    public void testDocumentNotExist() {
        Document a = new Document("_id", 1).append("value", 100);
        Document b = new Document("_id", 2).append("value", 50);
        dataCollection().drop();
        dataCollection().insertOne(a);
        // b is missing
        Transaction t = new TestTransaction(client(), a.get("_id"), b.get("_id"), 10);
        try {
            t.execute();
            fail ("should have raised an exception");
        } catch (RollbackException ex) {
            if (!ex.getMessage().startsWith("cause: cannot find document")) fail ("wrong exception");
            assertStatus(t, Status.CANCELLED);
        }
    }
    
    @Test
    public void testProcessingError() {
        Document a = new Document("_id", 1).append("value", 100);
        Document b = new Document("_id", 2).append("value", "dummy");
        dataCollection().drop();
        dataCollection().insertOne(a);
        dataCollection().insertOne(b);
        Transaction t = new TestTransaction(client(), a.get("_id"), b.get("_id"), 10);
        try {
            t.execute();
            fail ("should have raised a RollbackException");
        } catch (RollbackException ex) {
            // ok
        }
        assertValue(1, 100);
        assertValue(2, "dummy");
        assertStatus(t, Status.CANCELLED);
    }
    
    @Test
    public void testProcessingError2() {
        dataCollection().drop();
        CreateCollectionOptions o = new CreateCollectionOptions();
        o.autoIndex(true);
        ValidationOptions v = new ValidationOptions();
        v.validator(new Document("value", new Document("$lte", 100)));
        v.validationLevel(ValidationLevel.MODERATE);
        o.validationOptions(v);
        client().getDatabase(DATABASE).createCollection(DATA_COLLECTION, o);
        
        Document a = new Document("_id", 1).append("value", 20);
        Document b = new Document("_id", 2).append("value", 100);
        dataCollection().insertOne(a);
        dataCollection().insertOne(b);
        Transaction t = new TestTransaction(client(), a.get("_id"), b.get("_id"), 10);
        try {
            t.execute();
            fail ("should have raised a RollbackException");
        } catch (RollbackException ex) {
            // ok
        }
        assertValue(1,20);
        assertValue(2,100);
        assertStatus(t, Status.CANCELLED);
    }

    @Test
    public void testDocumentAlreadyLocked() {
        dataCollection().drop();
        Document a = new Document("_id", 1).append("value", 50);
        Document b = new Document("_id", 2).append("value", 70).append("lock", new Date());
        dataCollection().insertOne(a);
        dataCollection().insertOne(b);
        Transaction t = new TestTransaction(client(), a.get("_id"), b.get("_id"), 10);
        t.execute();

        assertValue(1, 40);
        assertValue(2, 80);
        assertStatus(t, Status.COMPLETE);
    }
    
}
