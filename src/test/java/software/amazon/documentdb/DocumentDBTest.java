package software.amazon.documentdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.Collections;

public class DocumentDBTest {

    /**
     * Adds a user in the dbOwner role to the admin database.
     *
     * @param databaseName The database the user will have access to.
     * @param username The user's username.
     * @param password The user's password.
     */
    public static void addUser(
            final String databaseName, final String username, final String password) {
        try (final MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            final MongoDatabase db = mongoClient.getDatabase("admin");
            final Document user =
                    new Document("createUser", username)
                            .append("pwd", password)
                            .append(
                                    "roles",
                                    Collections.singletonList(
                                            new Document("role", "dbOwner").append("db", databaseName)));
            db.runCommand(user);
        }
    }
}
