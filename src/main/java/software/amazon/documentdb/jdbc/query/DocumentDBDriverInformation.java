package software.amazon.documentdb.jdbc.query;

import com.mongodb.MongoDriverInformation;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

public class DocumentDBDriverInformation {
    static public MongoDriverInformation getMongoDriverInformation(final DocumentDbConnectionProperties documentDbConnectionProperties) {
        MongoDriverInformation mongoDriverInformation = MongoDriverInformation.builder()
                .driverName(documentDbConnectionProperties.getApplicationName())
                .driverVersion(documentDbConnectionProperties.getApplicationName())
                .build();
        return mongoDriverInformation;
    }
}
