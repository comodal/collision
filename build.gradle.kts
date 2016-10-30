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
  maven("http://oss.sonatype.org/content/repositories/snapshots")
  maven("https://jitpack.io")
  jcenter()
}

dependencies {
  testCompile("junit:junit:+")

  jmh("com.github.JCTools.JCTools:jctools-experimental:+")
  jmh("com.github.JCTools.JCTools:jctools-core:+")
  jmh("com.github.ben-manes.caffeine:caffeine:+")
  jmh("org.cache2k:cache2k-api:+")
  jmh("org.cache2k:cache2k-core:+")
}
