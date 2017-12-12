object Environment {
  val user = sys.env.get("USER").getOrElse("anon")
}
