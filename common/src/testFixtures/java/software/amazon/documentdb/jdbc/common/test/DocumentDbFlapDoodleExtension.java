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

import java.io.IOException;

/**
 * Jupiter extension for creating and configuring a FlapDoodle MongoDb instance with no replica set.
 */
public class DocumentDbFlapDoodleExtension extends DocumentDbFlapDoodleExtensionBase {
    private static final String BASIC_FLAPDOODLE_INFO = "basic-flapdoodle-info";

    private static class FlapDoodleHolder implements ServerHolder {
        FlapDoodleHolder() {
        }

        @Override
        public Integer getPort() {
            return getMongoPort();
        }

        @Override
        public void start() throws IOException {
            startMongoDbInstance(true);
        }

        @Override
        public void stop() {
            stopMongoDbInstance();
        }
    }

    @Override
    protected String getIdentifier() {
        return BASIC_FLAPDOODLE_INFO;
    }

    @Override
    protected ServerHolder createHolder() {
        return new FlapDoodleHolder();
    }
}
