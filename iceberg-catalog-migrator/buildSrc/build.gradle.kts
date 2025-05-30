/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

plugins { `kotlin-dsl` }

repositories {
  mavenCentral()
  gradlePluginPortal()
  if (System.getProperty("withMavenLocal").toBoolean()) {
    mavenLocal()
  }
}

dependencies {
  implementation(gradleKotlinDsl())
  implementation(baselibs.spotless)
  implementation(baselibs.jandex)
  implementation(baselibs.idea.ext)
  implementation(baselibs.shadow)
  implementation(baselibs.errorprone)
}

java {
  sourceCompatibility = JavaVersion.VERSION_21
  targetCompatibility = JavaVersion.VERSION_21
}

kotlinDslPluginOptions { jvmTarget.set(JavaVersion.VERSION_21.toString()) }

tasks.withType<Test>().configureEach { useJUnitPlatform() }
