import sbt.Def
import sbt.Keys._
import sbt._

object BuildSettings {

  val compilerFlags = Seq(
    "-deprecation",
    "-unchecked",
    "-Xexperimental",
    "-Xlint:_,-infer-any",
    "-feature",
    "-target:jvm-1.8"
  )

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")

  lazy val storeBintrayCredentials = taskKey[Unit]("Store bintray credentials.")
  lazy val credentialsFile: File = Path.userHome / ".bintray" / ".credentials"

  lazy val baseSettings: Seq[Def.Setting[_]] = GitVersion.settings

  lazy val buildSettings: Seq[Def.Setting[_]] = baseSettings ++ Seq(
    organization := "com.netflix.edda",
    scalaVersion := "2.11.12",
    scalacOptions ++= BuildSettings.compilerFlags,
    crossPaths := true,
    crossScalaVersions := Seq("2.11.12"),
    sourcesInBase := false,
    exportJars := true, // Needed for one-jar, with multi-project
    externalResolvers := BuildSettings.resolvers,
    checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value),
    storeBintrayCredentials := {
      IO.write(credentialsFile, bintray.BintrayCredentials.api.template(Bintray.user, Bintray.pass))
    }
  )

  val commonDeps: Seq[Nothing] = Seq.empty

  val resolvers = Seq(
    Resolver.mavenLocal,
    Resolver.jcenterRepo,
    "jfrog" at "https://oss.jfrog.org/oss-snapshot-local"
  )

  // Don't create root.jar, from:
  // http://stackoverflow.com/questions/20747296/producing-no-artifact-for-root-project-with-package-under-multi-project-build-in
  lazy val noPackaging = Seq(
    Keys.`package` := file(""),
    packageBin in Global := file(""),
    packagedArtifacts := Map()
  )

  def profile: Project => Project = p => {
    bintrayProfile(p)
      .settings(buildSettings: _*)
      .settings(libraryDependencies ++= commonDeps)
  }

  // Disable bintray plugin when not running under CI. Avoids a bunch of warnings like:
  //
  // ```
  // Missing bintray credentials /Users/brharrington/.bintray/.credentials. Some bintray features depend on this.
  // [warn] Credentials file /Users/brharrington/.bintray/.credentials does not exist
  // ```
  def bintrayProfile(p: Project): Project = {
    if (credentialsFile.exists)
      p.settings(Bintray.settings)
    else
      p.disablePlugins(bintray.BintrayPlugin)
  }
}
