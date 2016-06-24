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


  def createTransitive(transitive: LubmTransitive) = {

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
