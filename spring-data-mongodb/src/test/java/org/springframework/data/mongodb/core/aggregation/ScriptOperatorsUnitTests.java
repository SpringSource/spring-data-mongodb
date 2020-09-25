/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.aggregation.ScriptOperators.*;

import java.util.Collections;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * @author Christoph Strobl
 */
class ScriptOperatorsUnitTests {

	private static final String FUNCTION_BODY = "function(name) { return hex_md5(name) == \"15b0a220baa16331e8d80e15367677ad\" }";
	private static final Document EMPTY_ARGS_FUNCTION_DOCUMENT = new Document("body", FUNCTION_BODY)
			.append("args", Collections.emptyList()).append("lang", "js");

	@Test // DATAMONGO-2623
	void functionWithoutArgsShouldBeRenderedCorrectly() {

		assertThat(function(FUNCTION_BODY).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo($function(EMPTY_ARGS_FUNCTION_DOCUMENT));
	}

	@Test // DATAMONGO-2623
	void functionWithArgsShouldBeRenderedCorrectly() {

		assertThat(function(FUNCTION_BODY).args("$name").toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(
				$function(new Document(EMPTY_ARGS_FUNCTION_DOCUMENT).append("args", Collections.singletonList("$name"))));
	}

	static Document $function(Document source) {
		return new Document("$function", source);
	}
}
