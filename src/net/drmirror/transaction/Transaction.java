package net.drmirror.transaction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;

public abstract class Transaction {

    // INITIAL -> PREPARING -> APPLYING -> RELEASING -> COMPLETE // CANCELLING -> CANCELLED
    
    public enum Status {
        INITIAL, PENDING, APPLIED, CANCELLED
    }
    
    public static final String LOCK_FIELD = "lock";
    public static final int BACKOFF_MILLIS = 100;
    public static final int MAX_LOCK_MILLIS = 10000;

    protected MongoClient client;
    protected MongoCollection<Document> transactionCollection;

    protected Document transaction = null;
    protected List<DocRef> data = new ArrayList<DocRef>();

    private Status status = null;
    
    protected Transaction (MongoClient client, String dbName, String transactionCollectionName) {
        this.client = client;
        transactionCollection = client.getDatabase(dbName).getCollection(transactionCollectionName);
        this.status = Status.INITIAL;
        transaction = new Document("_id", new ObjectId());
    }
    
    protected void addDocument(String dbName, String collectionName, Object id) {
        data.add(new DocRef(dbName, collectionName, id));
    }
    
    protected void saveTransaction(Status newStatus) {
        transaction.put("status", newStatus.name().toLowerCase());
        transaction.put("ts", new Date());
        transactionCollection.replaceOne(
            new Document("_id", transaction.get("_id")),
            transaction,
            new UpdateOptions().upsert(true)
        );
        status = newStatus;
    }
   
    protected void lockDocuments() {
        for (DocRef d : data) {
            if (!lockDocument(d)) throw new RuntimeException("failed to lock document");
        }
    }
    
    protected void releaseDocuments() {
        for (DocRef d : data) {
            releaseDocument(d);
        }
    }

    protected boolean lockDocument (DocRef d) {
        Document result = null;
        while (true) {
            result = getCollection(d).findOneAndUpdate(
                new Document ("_id", d.getId()).append(LOCK_FIELD, new Document("$exists", false)),
                new Document ("$set", new Document (LOCK_FIELD, new Date())),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
            if (result != null) {
                d.setDocument(result);
                return true;
            } else {
                try {
                    checkStuckLock(d);
                    double jitter = 0.9 + 0.2 * Math.random();
                    int interval = (int)(jitter * (double)BACKOFF_MILLIS);
                    Thread.sleep(interval);
                } catch (InterruptedException ex) { 
                    throw new RuntimeException(ex);
                }
            }
        }
    }
    
    private void checkStuckLock (DocRef d) {
        Document x = getCollection(d).find(new Document("_id", d.getId())).first();
        if (x == null) {
            throw new RuntimeException("cannot find document while attempting to lock: _id="
                                       + d.getId());
        }
        Date ts = x.getDate(LOCK_FIELD);
        if (ts == null) {
            // lock has been cleared by somebody else
            return;
        } else {
            long age = System.currentTimeMillis() - ts.getTime();
            if (age > MAX_LOCK_MILLIS) {
                // lock is stuck, break it
                // because we also search by the timestamp we saw earlier,
                // it is guaranteed that only one process can succeed in breaking the lock
                getCollection(d).findOneAndUpdate(
                    new Document("_id", d.getId())
                         .append(LOCK_FIELD, ts),
                    new Document("$set", new Document(LOCK_FIELD, 1))
                );
            }
        }
    }
    
    protected void releaseDocument (DocRef d) {
        getCollection(d).findOneAndUpdate(
          new Document ("_id", d.getId()),
          new Document ("$unset", new Document (LOCK_FIELD, 1))
        );
    }
    
    private MongoCollection<Document> getCollection (DocRef d) {
        return client.getDatabase(d.getDbName()).getCollection(d.getCollectionName());
    }
    
    protected void backupDocuments() {
        List<Document> backup = new ArrayList<Document>();
        for (DocRef d : data) {
            backup.add(clone(d.find(client)));
        }
        transaction.put("backup", backup);
    }
    
    public Document clone (Document d) {
        Document result = new Document();
        for (Map.Entry<String,Object> e : d.entrySet()) {
            String s = e.getKey();
            Object v = e.getValue();
            if (v instanceof Document) {
                result.put(s, clone((Document)v));
            } else if (v instanceof List) {
                result.put(s, clone((List<Object>)v));
            } else {
                result.put(s, v);
            }
        }
        return result;
    }
    
    public List<Object> clone (List<Object> l) {
        List<Object> result = new ArrayList<Object>();
        for (Object o : l) {
            if (o instanceof Document) {
                result.add(clone((Document)o));
            } else if (o instanceof List) {
                result.add(clone((List<Object>)o));
            } else {
                result.add(o);
            }
        }
        return result;
    }
    
    public boolean execute() {
        if (status == null) {
            return false;
        } else {
            switch (status) {
            case INITIAL:
                backupDocuments();
                saveTransaction(Status.PENDING);
            case PENDING:
                try {
                    apply(getDocuments());
                    saveDocuments();
                    saveTransaction(Status.APPLIED);
                } catch (Exception ex) {
                    rollback();
                    throw new RollbackException(ex);
                }
            case APPLIED:
                return true;
            case CANCELLED:
                return false;
            }
        }
        return false;
    }

    public void rollback() {
        if (status == null) {
            return;
        } else {
            switch (status) {
            case INITIAL:
                return;
            case PENDING:
                cancel();
                saveTransaction(Status.CANCELLED);
            case APPLIED:
                return;
            case CANCELLED:
                return;
            }
        }
    }

    protected Document[] getDocuments() {
        Document[] result = new Document[data.size()];
        int i=0;
        for (DocRef d : data) {
            result[i++] = d.find(client);
        }
        return result;
    }
    
    public void saveDocuments() {
        for (DocRef d : data) {
            if (d.isModified()) d.save(client);
        }
    }
    
    public abstract void apply(Document... d);

    public void cancel() {
        List<Document> backup = (List<Document>)transaction.get("backup");
        for (int i=0; i<data.size(); i++) {
            data.get(i).replace(backup.get(i));
        }
        saveDocuments();
    };
        
}
