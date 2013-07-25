arowanaScala
========

Scala spinoff of the orginial arowana project

project documentation
1. install sbt
2. install intelliJ

(under browse repositories... in plugin management)
3. install intelliJ scala plugin
4. install intelliJ sbt plugin
5. change intelliJ color scheme to Dracula...
6. Download sbt project templage https://github.com/dph01/scala-sbt-template
7. unzip the template
8. modify build.sbt: look for line "libraryDependencies ++= { " then add appropriate dependencies
in this case   "org.scalaj" %% "scalaj-http" % "0.3.2" from     https://github.com/scalaj/scalaj-http/blob/master/README.textile
9. run ./sbt in the template folder
10. at sbt prompt run gen-idea
11. start intelliJ and open the template project (open not import)
12. try to Make project and get No SDK error.
13. configure some jdk1.6 (doesn't seem to matter..)
14. Make again and get error about production/test same output path
15. in Project Structure button, under modules then paths tab. make the paths different.
16. should work.
16.1. right-click on build.sbt file and select "Associate File type" and select Scala
    if you don't see this option, then *.sbt is already associated.
To export a standalone jar:
17. include the line
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.9.0")
to project/plugins.sbt
18. IMPORTANT uncomment the line
mainClass := Some("com.mycode.Curl")
in build.sbt so the jar knows where to enter the program.
19. in the project directory, run ./sbt
then type assembly to generate the jar
20. if you change project/plugin.sbt make sure
[error] Error parsing expression.  Ensure that settings are separated by blank lines.
TO INCLUDE scala-redis
21. copy source files from https://github.com/debasishg/scala-redis and append accordingly to currently project
22. modify project/ScalaRedisProject.scala, which is copied over from scala-redis:
change
  lazy val root = Project("RedisProject", file(".")) settings(coreSettings : _*)
to
  lazy val root = Project("arowana_scala", file(".")) settings(coreSettings : _*)
23. to update scala compiler for current project:
  start a new Scala project and enter scala hom directory and set it global.
  then go back to current project and change ur scala compiler
24. update library dependency in project/Build.scala and build.sbt: apparently 2.9 and 2.10 have different dependency formats
25. include scala spray: https://github.com/spray/spray-json