package net.drmirror.transaction;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class DocRef {

    private String dbName;
    private String collectionName;
    private Object _id;
    private Document d;
    private boolean isModified = false;
    
    public DocRef(String dbName, String collectionName, Object _id) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this._id = _id;
    }
    
    public String getDbName() { return dbName; };
    public String getCollectionName() { return collectionName; };
    public Object getId() { return _id; };
    public Document getDocument() { return d; };
    public void setDocument(Document d) { this.d = d; }
    
    public Document find (MongoClient c) {
        if (d == null) {
            d = find(c.getDatabase(dbName));
        }
        return d;
    }

    public Document find (MongoDatabase db) {
        if (d == null) {
          d = find(db.getCollection(collectionName));
        }
        return d;
    }
    
    public Document find (MongoCollection<Document> c) {
        if (d == null) {
          d = c.find(new Document("_id", _id)).first();
          if (d == null) throw new RuntimeException("document missing, _id=" + _id);
        }
        return d;
    }
    
    public void setModified(boolean b) {
        this.isModified = b;
    }
    
    public boolean isModified() {
        return isModified;
    }
    
    public void replace (Document d) {
        this.d = d;
        this.isModified = true;
    }
    
    public void save (MongoClient c) {
        if (d == null) return;
        save (c.getDatabase(dbName));
    }
    
    public void save (MongoDatabase db) {
        if (d == null) return;
        save (db.getCollection(collectionName));
        
    }
    
    public void save (MongoCollection<Document> c) {
        if (d == null) return;
        c.replaceOne(new Document("_id", _id), d);
        isModified = false;
    }
}
