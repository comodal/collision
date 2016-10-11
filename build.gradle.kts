buildscript {
  repositories {
    jcenter()
  }
}

apply {
  plugin("java")
  plugin("jmh")
}

configure<JavaPluginConvention> {
  setSourceCompatibility(9)
  setTargetCompatibility(9)
}

repositories {
  jcenter()
}

dependencies {
  testCompile("junit:junit:+")

  jmh("com.github.ben-manes.caffeine:caffeine:+")
  jmh("org.cache2k:cache2k-api:+")
  jmh("org.cache2k:cache2k-core:+")
}
