import Dependencies._

val gtVersion        = "1.2.0-SNAPSHOT"


organization := "com.azavea"
scalaVersion := "2.11.11"
version      := "0.1.0-SNAPSHOT"
name := "srtm-ingest"
libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark" % gtVersion,
  "org.locationtech.geotrellis" %% "geotrellis-s3" % gtVersion,
  "org.apache.spark"  %% "spark-core"    % "2.1.0" % Provided,
  "org.apache.hadoop"  % "hadoop-client" % "2.7.3" % Provided,
  scalaTest % Test
)

resolvers += "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots"

test in assembly := {}

assemblyShadeRules in assembly := {
  val shadePackage = "com.azavea.shaded.demo"
  Seq(
    ShadeRule.rename("com.google.common.**" -> s"$shadePackage.google.common.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-cassandra" % gtVersion).inAll,
    ShadeRule.rename("io.netty.**" -> s"$shadePackage.io.netty.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-hbase" % gtVersion).inAll,
    ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
      .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % gtVersion).inAll
  )
}

assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

sparkAwsRegion := "us-east-1"
sparkSubnetId := Some("subnet-4f553375")
sparkS3JarFolder := "s3://geotrellis-test/eac/srtm-ingest"
sparkInstanceCount := 51
sparkClusterName := "XTerrain collab tests"
sparkEmrRelease := "emr-5.4.0"
sparkEmrServiceRole := "EMR_DefaultRole"
sparkInstanceType := "m3.2xlarge"
sparkInstanceBidPrice := Some("0.5")
sparkInstanceRole := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value
  .withEc2KeyName("geotrellis-emr")

import sbtemrspark.EmrConfig
sparkEmrConfigs := Some(
  Seq(
    EmrConfig("spark").withProperties(
      "spark.driver-memory" -> "10000M",
      "spark.driver.cores" -> "4",
      "spark.executor.memory" -> "5120M",
      "spark.executor.cores" -> "2",
      "spark.driver.maxResultSize" -> "3g",
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.yarn.executor.memoryOverhead" -> "700M",
      "spark.yarn.driver.memoryOverhead" -> "0M"
    ),
    EmrConfig("yarn-site").withProperties(
      "yarn.resourcemanager.am.max-attempts" -> "1"
    )
  )
)
