/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.mongodb.core

import com.nhaarman.mockito_kotlin.verify
import example.first.First
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner

/**
 * Unit tests for [ExecutableUpdateOperationExtensions].
 *
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner::class)
class ExecutableUpdateOperationExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operation: ExecutableUpdateOperation

	@Test // DATAMONGO-1719
	fun `update(KClass) extension should call its Java counterpart`() {

		operation.update(First::class)
		verify(operation).update(First::class.java)
	}

	@Test // DATAMONGO-1719
	fun `update() with reified type parameter extension should call its Java counterpart`() {

		operation.update<First>()
		verify(operation).update(First::class.java)
	}
}
