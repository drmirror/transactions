package net.drmirror.transaction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

public abstract class Transaction {
    
    public enum Status {
        INITIAL, PENDING, APPLIED, CANCELLED
    }
    
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
