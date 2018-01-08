name                      := "srtm-ingest"
version                   := "0.2.0-SNAPSHOT"
organization              := "com.azavea"
scalaVersion in ThisBuild := "2.11.12"

val gtVersion = "2.0.0-SNAPSHOT"

scalacOptions := Seq(
  "-deprecation",
  "-Ypartial-unification",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

libraryDependencies ++= Seq(
  "com.azavea"                  %% "vectorpipe"       % "0.2.1",
  "com.monovore"                %% "decline"          % "0.4.0-RC1",
  "com.monovore"                %% "decline-refined"  % "0.4.0-RC1",
  "org.apache.hadoop"            % "hadoop-client"    % "2.7.3" % Provided,
  "org.apache.spark"            %% "spark-hive"       % "2.2.0" % Provided,
  "org.locationtech.geotrellis" %% "geotrellis-s3"    % gtVersion,
  "org.locationtech.geotrellis" %% "geotrellis-spark" % gtVersion,
  "org.scalatest"               %% "scalatest"        % "3.0.1" % Test,
  "org.typelevel"               %% "cats-core"        % "1.0.0-RC1",
  "org.apache.commons"           % "commons-math"     % "2.2",
  "org.typelevel"               %% "squants"          % "1.3.0"
)

resolvers += "LocationTech Snapshots" at "https://repo.locationtech.org/content/repositories/snapshots"
resolvers += "LocationTech Releases" at "https://repo.locationtech.org/content/repositories/releases"
resolvers += Resolver.bintrayRepo("azavea", "maven")

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


sparkInstanceCount          := 21
sparkInstanceType           := "m3.2xlarge"
sparkInstanceBidPrice       := Some("0.5")
sparkEmrRelease             := "emr-5.10.0"
sparkAwsRegion              := "us-east-1"
sparkSubnetId               := Some("subnet-4f553375")
sparkS3JarFolder            := s"s3://geotrellis-test/${Environment.user}/srtm-ingest"
sparkClusterName            := s"geotrellis-friction-surface - ${Environment.user}"
sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr")

import com.amazonaws.services.elasticmapreduce.model.Application
sparkRunJobFlowRequest      := sparkRunJobFlowRequest.value.withApplications(
  Seq("Spark", "Ganglia").map(a => new Application().withName(a)):_*
)


import sbtemrspark.EmrConfig
sparkEmrConfigs := Some(
  Seq(
    EmrConfig("spark").withProperties(
      "maximizeResourceAllocation" -> "true"
    ),
    EmrConfig("spark-defaults").withProperties(
      "spark.driver.maxResultSize" -> "3G",
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.shuffle.service.enabled" -> "true",
      "spark.shuffle.compress" -> "true",
      "spark.shuffle.spill.compress" -> "true",
      "spark.rdd.compress" -> "true",
      "spark.yarn.executor.memoryOverhead" -> "1G",
      "spark.yarn.driver.memoryOverhead" -> "1G",
      "spark.driver.maxResultSize" -> "3G",
      "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC -Dgeotrellis.s3.threads.rdd.write=64"

    ),
    EmrConfig("yarn-site").withProperties(
      "yarn.resourcemanager.am.max-attempts" -> "1",
      "yarn.nodemanager.vmem-check-enabled" -> "false",
      "yarn.nodemanager.pmem-check-enabled" -> "false"
    )
  )
)
