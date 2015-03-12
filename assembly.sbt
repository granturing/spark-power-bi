import AssemblyKeys._

assemblySettings

assemblyOption in assembly ~= { _.copy(includeScala = false) }
