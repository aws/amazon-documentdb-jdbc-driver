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

package software.amazon.documentdb.jdbc.calcite.adapter;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link DocumentDbRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class DocumentDbToEnumerableConverterRule extends ConverterRule {
    /** Singleton instance of MongoToEnumerableConverterRule. */
    public static final ConverterRule INSTANCE = Config.INSTANCE
            .withConversion(RelNode.class, DocumentDbRel.CONVENTION,
                    EnumerableConvention.INSTANCE, "MongoToEnumerableConverterRule")
            .withRuleFactory(DocumentDbToEnumerableConverterRule::new)
            .toRule(DocumentDbToEnumerableConverterRule.class);

    /** Called from the Config. */
    protected DocumentDbToEnumerableConverterRule(final Config config) {
        super(config);
    }

    @Override public RelNode convert(final RelNode rel) {
        final RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new DocumentDbToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
