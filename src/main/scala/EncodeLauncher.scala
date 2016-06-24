import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiangnanren on 23/06/16.
  */
object EncodeLauncher extends App {

  val conf = new SparkConf().setAppName("spark rdf").setMaster("local")
  val sc = new SparkContext(conf)

  val lubmConfig = LubmConfig(sc)

  val sameAs = new LubmSameAs("/Users/xiangnanren/IDEAWorkspace/" +
    "lubm-generator/data/lubm1_SameAs")
  val transitive = new LubmTransitive("/Users/xiangnanren/IDEAWorkspace/" +
    "lubm-generator/data/lubm1_Transitive")

  val lubm = new LubmFactory(lubmConfig)
  lubm.createSameAsType1(sameAs)
  lubm.createSameAsType2(sameAs)
  lubm.createTransitive(transitive)

}
