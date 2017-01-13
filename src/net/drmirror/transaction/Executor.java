package net.drmirror.transaction;

import java.util.Date;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

public class Executor {

    private MongoCollection<Document> transactions;
    private Document transaction = null;
    
    public boolean pickTransaction() {
        Document t = transactions.findOneAndUpdate(
                new Document("status","initial"),
                new Document("$set", new Document("status", "pending")
                                          .append("ts", new Date()))
        );
        if (t != null) {
            if (t.getString("status").equals("initial")) {
                this.transaction = t;
                return true;
            }
        }
        this.transaction = null;
        return false;
    }
    
    
    
}
