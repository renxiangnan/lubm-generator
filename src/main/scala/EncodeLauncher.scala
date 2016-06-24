import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiangnanren on 23/06/16.
  */
object EncodeLauncher extends App {

  val conf = new SparkConf().setAppName("spark rdf").setMaster("local")
  val sc = new SparkContext(conf)

  val lubmConfig = LubmConfig(sc)

  val sameAs = new LubmSameAs("/Users/xiangnanren/IDEAWorkspace/lubm-generator/data/lubm1_SameAs")

//  new LubmFactory(lubmConfig).createSameAsType1(sameAs)
  new LubmFactory(lubmConfig).createSameAsType2(sameAs)


}
