/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.common.test;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * Creates test environments.
 *
 * Note: Ensure the values set in the gradle.build match these enumeration values exactly.
 */
public class DocumentDbTestEnvironmentFactory {
    private static volatile ImmutableList<DocumentDbTestEnvironment> configuredEnvironments = null;

    /**
     * Gets the MongoDB 4.0 test environment.
     *
     * @return the {@link DocumentDbTestEnvironment} for the MongoDB 4.0 server.
     */
    public static DocumentDbTestEnvironment getMongoDb40Environment() {
        return DocumentDbTestEnvironmentType.MONGODB40_FLAPDOODLE.getEnvironment();
    }


    /**
     * Gets the DocumentDB 4.0 via SSH tunnel test environment.
     *
     * @return the {@link DocumentDbTestEnvironment} for the DocumentDB 4.0 server.
     */
    public static DocumentDbTestEnvironment getDocumentDb40SshTunnelEnvironment() {
        return DocumentDbTestEnvironmentType.DOCUMENTDB40_SSH_TUNNEL.getEnvironment();
    }

    /**
     * Gets the list of configured test environments using their default settings.
     *
     * @return a list of {@link DocumentDbTestEnvironment} for all configured test environments.
     */
    public static ImmutableList<DocumentDbTestEnvironment> getConfiguredEnvironments() {
        if (configuredEnvironments == null) {
            buildConfiguredEnvironments();
        }
        return configuredEnvironments;
    }

    private static synchronized void buildConfiguredEnvironments() {
        if (configuredEnvironments == null) {
            final String environmentNames = getEnvironmentNames();
            final ImmutableList.Builder<DocumentDbTestEnvironment> builder = ImmutableList.builder();
            Arrays.stream(environmentNames.split("[,]"))
                    .distinct()
                    .forEach(e -> builder.add(
                            DocumentDbTestEnvironmentType.valueOf(e).getEnvironment()));
            configuredEnvironments = builder.build();
        }
    }

    private static String getEnvironmentNames() {
        String environmentNames = System.getenv("CONFIGURED_ENVIRONMENTS");
        if (isNullOrWhitespace(environmentNames)) {
            environmentNames = DocumentDbTestEnvironmentType.MONGODB40_FLAPDOODLE.name();
        }
        return environmentNames;
    }

    private enum DocumentDbTestEnvironmentType {
        MONGODB40_FLAPDOODLE(new DocumentDbMongoTestEnvironment()),
        DOCUMENTDB40_SSH_TUNNEL(new DocumentDbDocumentDbTestEnvironment());

        private final DocumentDbTestEnvironment environment;

        DocumentDbTestEnvironmentType(final DocumentDbTestEnvironment environment) {
            this.environment = environment;
        }

        DocumentDbTestEnvironment getEnvironment() {
            return environment;
        }
    }
}
