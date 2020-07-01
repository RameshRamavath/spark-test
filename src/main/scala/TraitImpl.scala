object TraitImpl {

  def eitherExample(empName:String):Either[String,String]  ={
    if(!empName.isEmpty)
      Right(empName)
    else
      Left("please provide the name")
  }

  def tryCatch(emp: String): String ={
    try{
      if(!emp.isEmpty)
         emp
      else
        throw new Exception ("please provide the name")
    } catch {
      case exception: Exception => exception.getMessage
    }

  }


  def main(args: Array[String]): Unit = {
    println(eitherExample("Ramesh"))
    println(eitherExample(""))

    println(tryCatch("Ramesh"))
    println(tryCatch(""))

  }

}
