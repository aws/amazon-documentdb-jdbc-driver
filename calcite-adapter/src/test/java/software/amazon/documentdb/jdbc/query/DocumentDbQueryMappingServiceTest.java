package software.amazon.documentdb.jdbc.query;

import com.mongodb.client.MongoClient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.SQLException;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;

public class DocumentDbQueryMappingServiceTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static DocumentDbConnectionProperties connectionProperties;
    private static MongoClient client;

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    void setup() {
        connectionProperties = new DocumentDbConnectionProperties();
        createUser(DATABASE_NAME, USER, PASSWORD);
        connectionProperties.setUser(USER);
        connectionProperties.setPassword(PASSWORD);
        connectionProperties.setDatabase(DATABASE_NAME);
        connectionProperties.setTlsEnabled("false");
        connectionProperties.setHostname("localhost:" + getMongoPort());
        client = createMongoClient(ADMIN_DATABASE, USER, PASSWORD);
    }

    @AfterAll
    static void teardown() throws Exception {
        try (SchemaWriter schemaWriter = SchemaStoreFactory
                .createWriter(connectionProperties, client)) {
            schemaWriter.remove("id");
        }
        client.close();
    }

    protected void insertBsonDocuments(final String collectionName, final BsonDocument[] documents) {
        insertBsonDocuments(collectionName, DATABASE_NAME, documents, client);
    }

    protected DocumentDbQueryMappingService getQueryMappingService() throws SQLException {
        final DocumentDbDatabaseSchemaMetadata databaseMetadata =
                DocumentDbDatabaseSchemaMetadata.get(connectionProperties, "id", VERSION_NEW, client);
        return new DocumentDbQueryMappingService(connectionProperties, databaseMetadata, client);
    }

    protected static String getDatabaseName() {
        return DATABASE_NAME;
    }
}
