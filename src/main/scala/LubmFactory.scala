import java.io.{File, PrintWriter}


/**
  * Created by xiangnanren on 24/06/16.
  */
class LubmFactory(lubmConfig:LubmConfig) {

  def createSameAsType1(sameAs: LubmSameAs) = {
    val df1 = lubmConfig.df
    val writer = new PrintWriter(new File(sameAs.sameAsFile))
    var counter:Long = lubmConfig.maxIndividual

    df1.where(df1("p") <=> "0")
      .where(df1("o") <=> "973078528")
      .select("s", "p", "o").collect.foreach(row =>
      for (i <- 1 to sameAsBound) {
        counter += 1
        writer.write("("
        + row(0) + ","
        + "1" + ","
        + counter + ")\n")})
    writer.close()
  }


  def createSameAsType2(sameAs: LubmSameAs) = {
    val df1 = lubmConfig.df
    val writer = new PrintWriter(new File(sameAs.sameAsFile))
    var counter:Long = lubmConfig.maxIndividual
    var tempCounter: Long = counter

    df1.where(df1("p") <=> "0")
      .where(df1("o") <=> "973078528")
      .select("s", "p", "o").collect.foreach(row =>
      for (i <- 1 to sameAsBound) {
        if (i == 1) {
          counter += 1
          writer.write("("
            + row(0) + ","
            + "1" + ","
            + counter + ")\n")
        }
        else {
          tempCounter = counter + 1
          writer.write("("
          + counter + ","
          + "1" + ","
          + tempCounter + ")\n")
          counter = tempCounter
        }
      })
    writer.close()
  }

  // Chain construction
  // transitBound defines the diameter of chain
  def createTransitiveType1(transitive: LubmTransitive) = {
    val df1 = lubmConfig.df
    val writer = new PrintWriter(new File(transitive.transitiveFile))
    var counter: Long = 0L
    var i,j = 0

    val arrayRow = df1.where(df1("p") <=> "0")
      .where(df1("o") <=> "973078528")
      .select("s").collect

//    arrayRow.foreach(println(_))

    while(i < arrayRow.length ){
      while((j < transitBound) && (i+j+1 < arrayRow.length)){
        writer.write("("
          + arrayRow(i+j)(0) + ","
          + "2" + ","
          + arrayRow(i+j+1)(0) + ")\n")
        j += 1
      }
      j = 0
      i+=1
    }
    writer.close()
  }

  // Binary tree construction
  // transitBound defines degree of tree
  def createTransitiveType2(transitive: LubmTransitive) = {
    val df1 = lubmConfig.df
    val writer = new PrintWriter(new File(transitive.transitiveFile))
    var counter: Long = 0L

    var i,j = 0

    val arrayRow = df1.where(df1("p") <=> "0")
      .where(df1("o") <=> "973078528")
      .select("s").collect

    while(i < arrayRow.length ){
      while((j < transitBound) && (i+j+2 < arrayRow.length)){
        writer.write("("
          + arrayRow(i+j)(0) + ","
          + "2" + ","
          + arrayRow(i+j+1)(0) + ")\n"
          + arrayRow(i+j)(0) + ","
          + "2" + ","
          + arrayRow(i+j+2)(0) + ")\n")
        j += 2
      }
      i = i + j + 2
      j = 0
    }
    writer.close()
  }






  def sameAsBound: Int = {
        val r = scala.util.Random
        lubmConfig.sameAsLowBound + r.nextInt(
          lubmConfig.sameAsUpBound-lubmConfig.sameAsLowBound)
      }

  def transitBound: Int = {
      val r = scala.util.Random
      lubmConfig.transitLowBound + r.nextInt(
        lubmConfig.transitUpBound-lubmConfig.transitLowBound)
    }

}
